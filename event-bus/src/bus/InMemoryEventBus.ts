import { AbstractEventBus } from "./AbstractEventBus.ts";
import type { BaseEvent } from "./IEventBus.ts";

/**
 * A simple, process-local event bus implementation.
 *
 * - Stores subscribers in memory.
 * - Delivers events synchronously (via emit) or asynchronously (via emitAsync).

 * Limitations:
 * - Cannot distribute events across processes or machines.
 * - Subscribers disappear when the process ends.
 */
export class InMemoryEventBus<E extends BaseEvent> extends AbstractEventBus<E> {
  /**
   * Subscribe a handler to a specific event type.
   * 
   * @param eventType - The  event type to listen for.
   * @param handler - callback that processes the event payload.
   */
  subscribe<T extends E["type"]>(
    eventType: T,
    handler: (payload: Extract<E, { type: T }>["payload"]) => void | Promise<void>
  ): void {
    const handlers = this.subscribers.get(eventType) ?? [];
    handlers.push(handler);
    this.subscribers.set(eventType, handlers);
  }

  /**
   * Unsubscribe a previously registered handler.
   * 
   * @param eventType - The event type to unsubscribe from.
   * @param handler - The specific callback to remove.
   */
  unsubscribe<T extends E["type"]>(
    eventType: T,
    handler: (payload: Extract<E, { type: T }>["payload"]) => void | Promise<void>
  ): void {
    const handlers = this.subscribers.get(eventType) ?? [];
    this.subscribers.set(
      eventType,
      handlers.filter(h => h !== handler)
    );
  }

  /**
   * Emit an event synchronously.
   * 
   * - Immediately invokes all subscribed handlers in the current tick.
   * - If handlers are async, they are invoked but not awaited.
   */
  emit<T extends E["type"]>(event: Extract<E, { type: T }>): void {
    const handlers = this.subscribers.get(event.type);
    if (handlers) {
      for (const handler of handlers) {
        void handler(event.payload); // Fire-and-forget
      }
    }
  }

  /**
   * Emit an event asynchronously.
   * 
   * - Invokes all subscribed handlers and awaits their completion.
   * - Useful when you want to guarantee that all handlers finish before continuing.
   */
  async emitAsync<T extends E["type"]>(event: Extract<E, { type: T }>): Promise<void> {
    const handlers = this.subscribers.get(event.type);
    if (handlers) {
      await Promise.all(handlers.map(h => h(event.payload)));
    }
  }
}
