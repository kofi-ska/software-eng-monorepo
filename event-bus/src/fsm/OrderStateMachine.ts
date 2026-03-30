import { AbstractStateMachine } from "./AbstractStateMachine.ts";

/**
 * All possible states in the Amazon-like order flow.
 */
export type OrderState =
  | "INIT"
  | "PLACED"
  | "CONFIRMED"
  | "IN_TRANSIT"
  | "FILLED"
  | "FULFILLED"
  | "CANCELLED"
  | "FAILED_DELIVERY";

/**
 * Events that trigger transitions between order states.
 */
export type OrderEvent =
  | "PLACE_ORDER"
  | "CONFIRM"
  | "SHIP"
  | "FILL"
  | "FULFILL"
  | "CANCEL"
  | "FAIL_DELIVERY";

/**
 * Concrete FSM for Amazon-style order lifecycle.
 */
export class OrderStateMachine extends AbstractStateMachine<OrderState, OrderEvent> {
  protected transitions = {
    INIT: {
      PLACE_ORDER: "PLACED",
    },
    PLACED: {
      CONFIRM: "CONFIRMED",
      CANCEL: "CANCELLED",
    },
    CONFIRMED: {
      SHIP: "IN_TRANSIT",
    },
    IN_TRANSIT: {
      FILL: "FILLED",
      FAIL_DELIVERY: "FAILED_DELIVERY",
    },
    FILLED: {
      FULFILL: "FULFILLED",
    },
    FULFILLED: {},
    CANCELLED: {},
    FAILED_DELIVERY: {},
  } as const;
}
