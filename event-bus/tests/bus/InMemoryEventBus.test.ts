import { describe, it, expect, vi } from "vitest";
import { InMemoryEventBus } from "../../src/bus/InMemoryEventBus.js";
import type { BaseEvent } from "../../src/bus/IEventBus.js";

// Sample union of events for testing
type TestEvents =
  | { type: "PING"; payload: string }
  | { type: "NUMBER"; payload: number }
  | { type: "NOPAYLOAD" };

describe("InMemoryEventBus", () => {
  it("should call subscribed handler on emit", () => {
    const bus = new InMemoryEventBus<TestEvents>();
    const handler = vi.fn();

    bus.subscribe("PING", handler);
    bus.emit({ type: "PING", payload: "hello" });

    expect(handler).toHaveBeenCalledWith("hello");
  });

  it("should not call handler after unsubscribe", () => {
    const bus = new InMemoryEventBus<TestEvents>();
    const handler = vi.fn();

    bus.subscribe("PING", handler);
    bus.unsubscribe("PING", handler);
    bus.emit({ type: "PING", payload: "world" });

    expect(handler).not.toHaveBeenCalled();
  });

  it("should support multiple handlers for the same event", () => {
    const bus = new InMemoryEventBus<TestEvents>();
    const handler1 = vi.fn();
    const handler2 = vi.fn();

    bus.subscribe("NUMBER", handler1);
    bus.subscribe("NUMBER", handler2);
    bus.emit({ type: "NUMBER", payload: 42 });

    expect(handler1).toHaveBeenCalledWith(42);
    expect(handler2).toHaveBeenCalledWith(42);
  });

  it("should deliver events asynchronously with emitAsync", async () => {
    const bus = new InMemoryEventBus<TestEvents>();
    const handler = vi.fn().mockImplementation(
      async (msg: string) => new Promise(resolve => setTimeout(() => resolve(msg), 50))
    );

    bus.subscribe("PING", handler);
    await bus.emitAsync({ type: "PING", payload: "async-test" });

    expect(handler).toHaveBeenCalledWith("async-test");
  });

  it("should not throw if no handlers exist for an event", () => {
    const bus = new InMemoryEventBus<TestEvents>();
    expect(() => bus.emit({ type: "NOPAYLOAD" })).not.toThrow();
  });
});
