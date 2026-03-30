import type { BaseEvent, IEventBus } from "../bus/IEventBus.ts";
import type { IStateMachine } from "../fsm/IStateMachine.ts";

/**
 * A Saga listens to events and drives state transitions in a workflow.
 */
export interface ISaga<S, E, EV extends BaseEvent> {
  /**
   * Returns a unique name for this saga.
   */
  getName(): string;

  /**
   * Start listening to events and coordinating the saga.
   */
  start(bus: IEventBus<EV>): void;

  /**
   * Stop listening and clean up resources.
   */
  stop(): void;

  /**
   * (Optional) Returns the underlying state machine.
   */
  getStateMachine(): IStateMachine<S, E>;
}
