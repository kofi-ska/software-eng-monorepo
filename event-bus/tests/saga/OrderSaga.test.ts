import { describe, it, expect, vi, beforeEach } from "vitest";
import { InMemoryEventBus } from "../../src/bus/InMemoryEventBus.js";
import { OrderSaga } from "../../src/saga/OrderSaga.js";
import type { OrderState } from "../../src/fsm/OrderStateMachine.js";
import type { OrderEvent } from "../../src/fsm/OrderStateMachine.js";

type OrderSagaEvent =
  | { type: "PLACE_ORDER"; payload: { orderId: string } }
  | { type: "CONFIRM"; payload: { orderId: string } }
  | { type: "SHIP"; payload: { orderId: string } }
  | { type: "FILL"; payload: { orderId: string } }
  | { type: "FULFILL"; payload: { orderId: string } }
  | { type: "CANCEL"; payload: { orderId: string } }
  | { type: "FAIL_DELIVERY"; payload: { orderId: string } };

describe("OrderSaga", () => {
  let bus: InMemoryEventBus<OrderSagaEvent>;
  let saga: OrderSaga;

  beforeEach(() => {
    bus = new InMemoryEventBus<OrderSagaEvent>();
    saga = new OrderSaga();
    saga.start(bus);
  });

  it("should follow the full happy path", async () => {
    const emitted: OrderSagaEvent[] = [];

    // Spy on emitted events
    for (const type of ["CONFIRM", "SHIP", "FILL", "FULFILL"] as OrderEvent[]) {
      bus.subscribe(type, payload => {
        emitted.push({ type, payload });
      });
    }

    await bus.emitAsync({ type: "PLACE_ORDER", payload: { orderId: "123" } });
    await Promise.resolve(); // flush async

    expect(saga.getStateMachine().getState()).toBe("FULFILLED");
    expect(emitted.map(e => e.type)).toEqual(["CONFIRM", "SHIP", "FILL", "FULFILL"]);
  });

  it("should handle manual cancellation after PLACED", async () => {
    const emitted: string[] = [];

    bus.subscribe("CANCEL", payload => {
      emitted.push("CANCEL");
    });

    bus.subscribe("CONFIRM", payload => {
      emitted.push("CONFIRM"); // should not be called
    });

    await bus.emitAsync({ type: "PLACE_ORDER", payload: { orderId: "456" } });
    await bus.emitAsync({ type: "CANCEL", payload: { orderId: "456" } });

    expect(saga.getStateMachine().getState()).toBe("CANCELLED");
    expect(emitted).toContain("CANCEL");
    expect(emitted).not.toContain("CONFIRM");
  });

  it("should reject invalid transitions and stay in same state", async () => {
    const initialState: OrderState = saga.getStateMachine().getState();
    await bus.emitAsync({ type: "SHIP", payload: { orderId: "000" } });

    expect(saga.getStateMachine().getState()).toBe(initialState); // still INIT
  });

  it("should handle failed delivery scenario", async () => {
    const fsm = saga.getStateMachine();

    await bus.emitAsync({ type: "PLACE_ORDER", payload: { orderId: "789" } });
    await bus.emitAsync({ type: "CONFIRM", payload: { orderId: "789" } });
    await bus.emitAsync({ type: "SHIP", payload: { orderId: "789" } });
    await bus.emitAsync({ type: "FAIL_DELIVERY", payload: { orderId: "789" } });

    expect(fsm.getState()).toBe("FAILED_DELIVERY");
  });

  it("should stop listening after stop() is called", async () => {
    saga.stop();
    const fsm = saga.getStateMachine();

    await bus.emitAsync({ type: "PLACE_ORDER", payload: { orderId: "stop-test" } });

    expect(fsm.getState()).toBe("INIT"); // unchanged
  });
});
