/**
 * Base shape of an event in the bus.
 * Every event must have a string `type` and an optional `payload`.
 */
export interface BaseEvent {
  type: string;
  payload?: any;
}

/**
 * IEventBus defines the contract for any event bus implementation.
 * Supports subscribing, unsubscribing, emitting sync and async events.
 *
 * Generic type parameter E extends BaseEvent,
 * so TS knows events always have `type` and `payload`.
 */
export interface IEventBus<E extends BaseEvent> {
  subscribe<T extends E["type"]>(
    eventType: T,
    handler: (payload: Extract<E, { type: T }>["payload"]) => void | Promise<void>
  ): void;

  unsubscribe<T extends E["type"]>(
    eventType: T,
    handler: (payload: Extract<E, { type: T }>["payload"]) => void | Promise<void>
  ): void;

  emit<T extends E["type"]>(
    event: Extract<E, { type: T }>
  ): void;

  emitAsync<T extends E["type"]>(
    event: Extract<E, { type: T }>
  ): Promise<void>;
}
