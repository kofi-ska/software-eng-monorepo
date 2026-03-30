import { InMemoryEventBus } from "./bus/InMemoryEventBus.ts";
import { OrderSaga } from "./saga/OrderSaga.ts";
import type { OrderSagaEvent } from "./saga/OrderSaga.ts";
/**
 * This demo simulates a full Order lifecycle using the in-memory event bus and saga.
 */

async function main() {
  const bus = new InMemoryEventBus<OrderSagaEvent>();
  const saga = new OrderSaga();

  saga.start(bus);

  const orderId = "ORDER-001";

  // Simulate starting the flow
  await bus.emitAsync({ type: "PLACE_ORDER", payload: { orderId } });

  // Wait for all chained events to process
  await new Promise((resolve) => setTimeout(resolve, 50));

  const finalState = saga.getStateMachine().getState();
  console.log(`[App] Final state for ${orderId}: ${finalState}`);
}

main().catch(console.error);
