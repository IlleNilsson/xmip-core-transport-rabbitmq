# xmip-core-transport-rabbitmq

RabbitMQ transport: RabbitMQ's idiom over the amqp technology's AMQP 0-9-1 — a queue is a Location, published to through the default exchange, declared durable first and every message persistent — and every delivery is one Stream. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

The protocol is not written here. The frame, the methods, the content, the
client and the far-end session are
[xmip-core-transport-amqp](https://github.com/IlleNilsson/xmip-core-transport-amqp)'s,
the one AMQP 0-9-1 in the estate; this crate keeps what is RabbitMQ's own:
the queue as the Location, the `rabbitmq://` URIs, and declaring and
persisting before it publishes. Who a Location logs in as is the amqp
technology's `Presenting` too: its `user`, with the password of the secret
it names, read as each connection opens (ADR-0019, amendment 2026-11-25);
the broker's default guest where it names no `user`. Until 2026-11-25 a
Location naming a secret was refused as the node started.

A Send Location publishes on a connection kept per broker (`transport::Pool`), its channel in confirm mode and each queue declared on it once; a publish returns once the broker confirms it. Until 2026-09-27 every send connected, declared, published without a confirm and closed.

A Receive Location declares and consumes its queue once, on its first receive, through the amqp technology's `Client::consuming`, and the consumer stays attached between receives; each receive takes what came until the broker is quiet for the timeout. A consumer the broker closed is replaced. Until 2026-09-28 every receive connected, declared, consumed and closed.

A delivery is acknowledged after the runtime's whole receive cycle, never on its own, by the amqp technology's `acknowledging`: `basic.ack` on the consumer that received it when the cycle accepted it, `basic.reject` without requeue when it refused it, so `RabbitMQ` drops it, or dead-letters it where the queue has a dead-letter exchange, and never delivers it again, and `basic.reject` with requeue when the cycle failed, so `RabbitMQ` delivers it again. Where the broker closed that consumer meanwhile no other is opened: `RabbitMQ` requeued every delivery that consumer had not answered, and delivers it again. The answer is one frame written on the kept connection, nothing waited for. Until 2026-10-02 a delivery was acknowledged as it was received.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

A Send Location publishes to the queue its `queue` setting names, on the broker its address names: it sends to the empty target (`Configured::own_target`, `transport::destination::OWN`), which `transport::destination::on_server` reads as that queue. A target naming another still wins — `invoices`, or `rabbitmq://host:5672/invoices` (ADR-0064, amendment 2026-11-10).

## The deduplication key

A keyed send (the key `Transport::send_from` is handed, `Outgoing::key`; built 2026-10-04) carries the Journey's identifier as the message's message-id property, through the amqp technology's `Properties::keyed`, the same on every attempt on one Journey. `RabbitMQ` itself appends a repeated publish; a consumer recognizes it by its message-id, and the message deduplication plugin, where a queue is declared with it, drops it. An unkeyed `send` sets none.

## The declaration

What a Location of this technology is configured with is two `const`s in its `declaration` module, `src/declaration.rs`, beside the code, which `Configured` reads (ADR-0064, amendment 2026-11-26): `declaration::SETTINGS`, `queue`, `virtual_host`, `user` and `timeout`, and `SCHEMES`, `rabbitmq`; `user` and the name `virtual_host` are the amqp technology's `presenting`, which this crate's declaration takes by that crate's own `declaration` feature. Built with its `declaration` feature alone (`--no-default-features --features declaration`), the crate is those and nothing more: its code and every crate the code is built on — the amqp technology's code among them — are behind the default feature. So the library the surfaces load can carry it without the code, for the language server and the desktop to suggest and check a RabbitMQ Location's settings though no node in their process loaded the technology, once the gathering crate of open problem 48 links it. `test/Declaration.Test.ps1` in the estate holds that the declaration builds nothing the transport capability does not.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
