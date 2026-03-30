
# Event Bus (TypeScript)

A simple event bus implementation in TypeScript with:

- In-memory event bus for local development or tests.
- Redis-based event bus for pub/sub across processes.
- Optional Saga orchestration and state machines.

## Features
- Publish/subscribe to typed events.
- Pluggable backends (in-memory or Redis).
- Async event handling.
- Built-in example saga: Amazon-style order flow.


## Project Structure

- `src/bus/` — Event bus implementations.

- `InMemoryEventBus.ts`
- `RedisEventBus.ts`
- `src/fsm/` — State machine and order flow example.
- `src/saga/` — Saga orchestration.
- `src/index.ts` — Entry point using in-memory bus.
- `src/index.redis.ts` — Entry point using Redis bus.

## Prerequisites

- Node.js 20+
- npm
- Docker (for Redis version)

## Installation

```bash

npm  install

```

Run tests:

```bash

npm  test

```

## Usage

### 1. In-Memory Version

Runs everything inside a single Node.js process.

```bash

npm  run  start

```

This uses `src/index.ts` and the `InMemoryEventBus`.


Expected console output:

```

[Saga] PLACE_ORDER → new state: PLACED

[Saga] CONFIRM → new state: CONFIRMED

...

[App] Final state for ORDER-001: FULFILLED

```


### 2. Redis Version (Docker Compose)

Runs Redis and the app with `RedisEventBus` to simulate pub/sub across processes.


1. Copy the example environment file:

```bash

cp .env.example .env

```


2. Start with Docker Compose:

```bash

docker compose up --build

```



3. The app will run `src/index.redis.ts` and connect to Redis at `${REDIS_URL}`.

Expected console output:

```
[Saga] PLACE_ORDER → new state: PLACED

[Saga] CONFIRM → new state: CONFIRMED
...

[App] Final state for ORDER-REDIS-001: FULFILLED

```

## Environment Variables

- `REDIS_URL` — Redis connection string (default: `redis://redis:6379` in Docker).

## Development

- Modify or add events in `OrderSaga` and `OrderStateMachine` for new workflows.

- Add new bus implementations by extending `AbstractEventBus`.

