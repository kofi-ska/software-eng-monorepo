import { describe, it, expect } from "vitest";
import { OrderStateMachine, type OrderState, type OrderEvent } from "../../src/fsm/OrderStateMachine.js";

describe("OrderStateMachine", () => {
  it("should start in INIT state", () => {
    const fsm = new OrderStateMachine("INIT");
    expect(fsm.getState()).toBe("INIT");
  });

  it("should allow valid transitions", () => {
    const fsm = new OrderStateMachine("INIT");
    fsm.transition("PLACE_ORDER");
    expect(fsm.getState()).toBe("PLACED");

    fsm.transition("CONFIRM");
    expect(fsm.getState()).toBe("CONFIRMED");

    fsm.transition("SHIP");
    expect(fsm.getState()).toBe("IN_TRANSIT");

    fsm.transition("FILL");
    expect(fsm.getState()).toBe("FILLED");

    fsm.transition("FULFILL");
    expect(fsm.getState()).toBe("FULFILLED");
  });

  it("should reject invalid transitions", () => {
    const fsm = new OrderStateMachine("PLACED");
    expect(() => fsm.transition("SHIP")).toThrow("Invalid transition from PLACED using event 'SHIP'");

    const fsm2 = new OrderStateMachine("CONFIRMED");
    expect(() => fsm2.transition("CONFIRM")).toThrow("Invalid transition from CONFIRMED using event 'CONFIRM'");
  });

  it("should transition to CANCELLED", () => {
    const fsm = new OrderStateMachine("PLACED");
    fsm.transition("CANCEL");
    expect(fsm.getState()).toBe("CANCELLED");
  });

  it("should transition to FAILED_DELIVERY", () => {
    const fsm = new OrderStateMachine("IN_TRANSIT");
    fsm.transition("FAIL_DELIVERY");
    expect(fsm.getState()).toBe("FAILED_DELIVERY");
  });

  it("should not allow any transitions from terminal states", () => {
    const terminalStates: OrderState[] = ["FULFILLED", "CANCELLED", "FAILED_DELIVERY"];
    const allEvents: OrderEvent[] = [
      "PLACE_ORDER",
      "CONFIRM",
      "SHIP",
      "FILL",
      "FULFILL",
      "CANCEL",
      "FAIL_DELIVERY"
    ];

    for (const state of terminalStates) {
      const fsm = new OrderStateMachine(state);
      for (const event of allEvents) {
        expect(fsm.canTransition(event)).toBe(false);
        expect(() => fsm.transition(event)).toThrow();
      }
    }
  });
});