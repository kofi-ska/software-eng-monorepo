import type { BaseEvent, IEventBus } from "../bus/IEventBus.ts";
import type { ISaga } from "./ISaga.ts";
import type { IStateMachine } from "../fsm/IStateMachine.ts";

/**
 * Abstract base for saga orchestration logic.
 */
export abstract class AbstractSaga<S, E, EV extends BaseEvent> implements ISaga<S, E, EV> {
  protected readonly sagaName: string;
  protected readonly stateMachine: IStateMachine<S, E>;
  protected readonly subscriptions: Map<string, (payload: any) => void | Promise<void>> = new Map();

  constructor(name: string, stateMachine: IStateMachine<S, E>) {
    this.sagaName = name;
    this.stateMachine = stateMachine;
  }

  getName(): string {
    return this.sagaName;
  }

  getStateMachine(): IStateMachine<S, E> {
    return this.stateMachine;
  }

  abstract start(bus: IEventBus<EV>): void;

  abstract stop(): void;
}
