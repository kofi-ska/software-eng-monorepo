/**
 * Generic interface for a finite state machine (FSM).
 * E = event type triggering transition.
 * S = state type (string union or enum).
 */
export interface IStateMachine<S, E> {
    /**
     * Returns the current state of the FSM.
     */
    getState(): S;

    /**
     * Transitions the FSM to the next state based on the event.
     * If the transition is invalid, it throws.
     * 
     * @param event - Trigger event to process.
     */
    transition(event: E): void;

    /**
     * Checks if the transition is valid without applying it.
     */
    canTransition(event: E): boolean;
}
