import { AbstractEventBus } from "./AbstractEventBus.ts";
import type { BaseEvent } from "./IEventBus.ts";
import { createClient, type RedisClientType } from "redis";

/**
 * RedisEventBus
 * A distributed event bus implementation using Redis Pub/Sub.
 *
 * - Uses two Redis clients:
 *    - Publisher: sends events.
 *    - Subscriber: listens for events.
 * - Supports multi-process and multi-machine setups.
 * - Events are serialized as JSON before being published.

 */
export class RedisEventBus<E extends BaseEvent> extends AbstractEventBus<E> {
  private pubClient: RedisClientType;
  private subClient: RedisClientType;

  constructor(redisUrl: string = process.env.REDIS_URL || "redis://localhost:6379") {
    super();
    this.pubClient = createClient({ url: redisUrl });
    this.subClient = createClient({ url: redisUrl });
  }

  /**
   * Connects publisher and subscriber clients to Redis.
   * 
   * - Must be called before emitting or subscribing.
   * - Also replays current subscriptions to Redis (if any were added pre-connect).
   */
  async connect(): Promise<void> {
    await this.pubClient.connect();
    await this.subClient.connect();

    // Attach Redis subscriptions for 
    // already-registered event types
    for (const eventType of this.subscribers.keys()) {
      await this.subClient.subscribe(eventType, (message: string) => {
        const event = JSON.parse(message) as E;
        const handlers = this.subscribers.get(event.type);
        if (handlers) {
          handlers.forEach(h => void h(event.payload));
        }
      });
    }
  }

  /**
   * Subscribe a handler to a given event type.
   * 
   * - Stores the handler locally.
   * - If Redis is connected, subscribes to the channel immediately.
   */
  subscribe<T extends E["type"]>(
    eventType: T,
    handler: (payload: Extract<E, { type: T }>["payload"]) => void | Promise<void>
  ): void {
    const handlers = this.subscribers.get(eventType) ?? [];
    handlers.push(handler);
    this.subscribers.set(eventType, handlers);

    if (this.subClient.isOpen) {
      void this.subClient.subscribe(eventType, (message: string) => {
        const event = JSON.parse(message) as E;
        void handler(event.payload);
      });
    }
  }

  /**
   * Unsubscribe a specific handler.
   * 
   * - Note: Redis Pub/Sub unsubscribes the whole channel, not individual handlers.
   * - We handle filtering locally to remove only the given handler.
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
    // Redis channel stays subscribed as long as at least one handler exists.
  }

  /**
   * Emit an event synchronously.
   * 
   * - Fires all local handlers immediately.
   * - Publishes to Redis asynchronously (fire-and-forget).
   */
  emit<T extends E["type"]>(event: Extract<E, { type: T }>): void {
    const handlers = this.subscribers.get(event.type);
    if (handlers) {
      for (const handler of handlers) {
        void handler(event.payload);
      }
    }

    void this.pubClient.publish(event.type, JSON.stringify(event));
  }

  /**
   * Emit an event asynchronously.
   * 
   * - Invokes local handlers and awaits their completion.
   * - Publishes to Redis and waits for publish acknowledgement.
   */
  async emitAsync<T extends E["type"]>(event: Extract<E, { type: T }>): Promise<void> {
    const handlers = this.subscribers.get(event.type);
    if (handlers) {
      await Promise.all(handlers.map(h => h(event.payload)));
    }

    await this.pubClient.publish(event.type, JSON.stringify(event));
  }
}
