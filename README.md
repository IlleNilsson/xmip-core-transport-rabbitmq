# xmip-core-transport-rabbitmq

RabbitMQ transport: RabbitMQ's idiom over the amqp technology's AMQP 0-9-1 — a queue is a Location, published to through the default exchange, declared durable first and every message persistent — and every delivery is one Stream. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

The protocol is not written here. The frame, the methods, the content, the
client and the far-end session are
[xmip-core-transport-amqp](https://github.com/IlleNilsson/xmip-core-transport-amqp)'s,
the one AMQP 0-9-1 in the estate; this crate keeps what is RabbitMQ's own:
the queue as the Location, the `rabbitmq://` URIs, and declaring and
persisting before it publishes.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
