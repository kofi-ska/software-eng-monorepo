import type { BaseEvent, IEventBus } from "../bus/IEventBus.ts";
import { AbstractSaga } from "./AbstractSaga.ts";
import { OrderStateMachine, type OrderState, type OrderEvent } from "../fsm/OrderStateMachine.ts";

/**
 * Defines the concrete event types that this saga handles.
 */
export type OrderSagaEvent =
  | { type: "PLACE_ORDER"; payload: { orderId: string } }
  | { type: "CONFIRM"; payload: { orderId: string } }
  | { type: "SHIP"; payload: { orderId: string } }
  | { type: "FILL"; payload: { orderId: string } }
  | { type: "FULFILL"; payload: { orderId: string } }
  | { type: "CANCEL"; payload: { orderId: string } }
  | { type: "FAIL_DELIVERY"; payload: { orderId: string } };

export class OrderSaga extends AbstractSaga<OrderState, OrderEvent, OrderSagaEvent> {
  private bus: IEventBus<OrderSagaEvent> | null = null;

  constructor() {
    super("OrderSaga", new OrderStateMachine("INIT"));
  }

  /**
   * Starts listening to events on the bus and handles state transitions.
   */
  start(bus: IEventBus<OrderSagaEvent>): void {
    this.bus = bus;

    const allEvents: OrderSagaEvent["type"][] = [
      "PLACE_ORDER", "CONFIRM", "SHIP", "FILL", "FULFILL", "CANCEL", "FAIL_DELIVERY"
    ];

    for (const type of allEvents) {
      const handler = async (payload: any) => {
        try {
          this.stateMachine.transition(type);
          console.log(`[Saga] ${type} → new state: ${this.stateMachine.getState()}`);

          const nextEvent = this.getNextEvent(type);
          if (nextEvent && this.bus) {
            await this.bus.emitAsync({ type: nextEvent, payload: { orderId: payload.orderId } });
          }
        } catch (err) {
          console.error(`[Saga] Invalid transition: ${type} — ${err}`);
        }
      };

      this.subscriptions.set(type, handler);
      bus.subscribe(type, handler);
    }
  }

  /**
   * Stops listening to all events.
   */
  stop(): void {
    if (!this.bus) return;

    for (const [type, handler] of this.subscriptions.entries()) {
      this.bus.unsubscribe(type as OrderSagaEvent["type"], handler);
    }

    this.subscriptions.clear();
    this.bus = null;
  }

  /**
   * Returns the next event to emit, given the current transition.
   */
  private getNextEvent(current: OrderEvent): OrderEvent | null {
    const chain: Record<OrderEvent, OrderEvent | null> = {
      PLACE_ORDER: "CONFIRM",
      CONFIRM: "SHIP",
      SHIP: "FILL",
      FILL: "FULFILL",
      FULFILL: null,
      CANCEL: null,
      FAIL_DELIVERY: null,
    };

    return chain[current] ?? null;
  }
}
