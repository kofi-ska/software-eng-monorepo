import type { IEventBus, BaseEvent } from "./IEventBus.ts";

/**
 * AbstractEventBus provides a base implementation for managing subscribers.
 * Concrete buses extend this class and implement the actual delivery logic.
 */
export abstract class AbstractEventBus<E extends BaseEvent> implements IEventBus<E> {
  protected subscribers: Map<string, Array<(payload: any) => void | Promise<void>>> = new Map();

  abstract subscribe<T extends E["type"]>(
    eventType: T,
    handler: (payload: Extract<E, { type: T }>["payload"]) => void | Promise<void>
  ): void;

  abstract unsubscribe<T extends E["type"]>(
    eventType: T,
    handler: (payload: Extract<E, { type: T }>["payload"]) => void | Promise<void>
  ): void;

  abstract emit<T extends E["type"]>(
    event: Extract<E, { type: T }>
  ): void;

  abstract emitAsync<T extends E["type"]>(
    event: Extract<E, { type: T }>
  ): Promise<void>;
}
