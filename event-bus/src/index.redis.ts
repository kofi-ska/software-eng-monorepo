import "dotenv/config";
import { RedisEventBus } from "./bus/RedisEventBus.ts";
import { OrderSaga } from "./saga/OrderSaga.ts";
import type { OrderSagaEvent } from "./saga/OrderSaga.ts";



const bus = new RedisEventBus<OrderSagaEvent>(process.env.REDIS_URL);

const saga = new OrderSaga();
await bus.connect();
saga.start(bus);

await bus.emitAsync({
  type: "PLACE_ORDER",
  payload: { orderId: "ORDER-REDIS-001" }
});
