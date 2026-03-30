import type { IStateMachine } from "./IStateMachine.ts";

/**
 * Base FSM class that uses a state map to validate transitions.
 */
export abstract class AbstractStateMachine<S extends string, E extends string> implements IStateMachine<S, E> {
  protected currentState: S;

  /**
   * Maps a current state to a map of valid events and resulting states.
   * Example:
   * {
   *   PLACED: { CONFIRM: CONFIRMED },
   *   CONFIRMED: { SHIP: IN_TRANSIT }
   * }
   */
  protected abstract transitions: Record<S, Partial<Record<E, S>>>;

  constructor(initialState: S) {
    this.currentState = initialState;
  }

  getState(): S {
    return this.currentState;
  }

  canTransition(event: E): boolean {
    const validTransitions = this.transitions[this.currentState];
    return !!validTransitions && event in validTransitions;
  }

  transition(event: E): void {
    if (!this.canTransition(event)) {
      throw new Error(`Invalid transition from ${this.currentState} using event '${event}'`);
    }

    const nextState = this.transitions[this.currentState][event];
    if (!nextState) {
      throw new Error(`Transition map incomplete for ${this.currentState} + ${event}`);
    }

    this.currentState = nextState;
  }
}
