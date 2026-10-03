# xmip-core-transport-rabbitmq

RabbitMQ transport: RabbitMQ's idiom over the amqp technology's AMQP 0-9-1 — a queue is a Location, published to through the default exchange, declared durable first and every message persistent — and every delivery is one Stream. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

The protocol is not written here. The frame, the methods, the content, the
client and the far-end session are
[xmip-core-transport-amqp](https://github.com/IlleNilsson/xmip-core-transport-amqp)'s,
the one AMQP 0-9-1 in the estate; this crate keeps what is RabbitMQ's own:
the queue as the Location, the `rabbitmq://` URIs, and declaring and
persisting before it publishes.

A Send Location publishes on a connection kept per broker (`transport::Pool`), its channel in confirm mode and each queue declared on it once; a publish returns once the broker confirms it. Until 2026-09-27 every send connected, declared, published without a confirm and closed.

A Receive Location declares and consumes its queue once, on its first receive, through the amqp technology's `Client::consuming`, and the consumer stays attached between receives; each receive takes what came until the broker is quiet for the timeout. A consumer the broker closed is replaced. Until 2026-09-28 every receive connected, declared, consumed and closed.

A delivery is acknowledged after the runtime's whole receive cycle, never on its own, by the amqp technology's `acknowledging`: `basic.ack` on the consumer that received it when the cycle accepted it, `basic.reject` without requeue when it refused it, so `RabbitMQ` drops it, or dead-letters it where the queue has a dead-letter exchange, and never delivers it again, and `basic.reject` with requeue when the cycle failed, so `RabbitMQ` delivers it again. Where the broker closed that consumer meanwhile no other is opened: `RabbitMQ` requeued every delivery that consumer had not answered, and delivers it again. The answer is one frame written on the kept connection, nothing waited for. Until 2026-10-02 a delivery was acknowledged as it was received.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
