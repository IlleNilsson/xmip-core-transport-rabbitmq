#![forbid(unsafe_code)]

//! Streams that arrive as deliveries from a `RabbitMQ` queue. One
//! basic.deliver is one Stream, the queue kept beside it.
//!
//! `RabbitMQ` listens for AMQP 0-9-1 on port 5672, and a queue is the
//! Location: a Receive Location connects with PLAIN, declares its queue
//! durable, consumes it and acknowledges each delivery after the runtime's
//! receive cycle — `basic.ack` when accepted, `basic.reject` without
//! requeue when refused, with requeue when the cycle failed; a Send
//! Location declares the queue and publishes to it through the
//! default exchange under the queue's own name, with a content header that
//! marks the message persistent. Either may instead accept clients directly
//! through the `amqp` technology's `Session`, one client's worth of broker
//! on one channel, which is what the playground stands up in place of a
//! broker.
//!
//! The protocol is the `amqp` technology's — the frame, the methods, the
//! content, the client and the session are written once, there. What is
//! here is `RabbitMQ`'s idiom: the queue as the Location, the default
//! exchange, the durable declaration before a publish, persistence, and
//! the `rabbitmq://` URIs. The `amqp` technology speaks the same protocol
//! to any broker with an exchange and a routing key as its Location.
//!
//! A send target is `rabbitmq://host:5672/orders`, `rabbitmq://host:5672`
//! for this transport's queue on another broker, or a queue name alone on
//! this transport's broker. The origin URI carries what the frame knew:
//! `rabbitmq://broker/orders?delivery-tag=1`.

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::time::Duration;

use amqp::content::Properties;
use amqp::{Client, Credentials, Session, acknowledging};
use net::Target;
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::pool::delivered;
use transport::socket;
use transport::{Arrived, Configured, Directions, Pool, Pooled, Taken, Transport};
use xcore::settings::{Applies, Kind, Presence, Read, Setting, Settings};

#[derive(Clone)]
pub struct RabbitMqTransport {
    broker: String,
    queue: String,
    credentials: Credentials,
    timeout: Option<Duration>,
    /// The connections a send publishes on: connected once per broker and
    /// kept, each queue declared on each once.
    publishers: Pool<Publisher>,
    /// The connection a receive takes from, its queue declared and
    /// consumed on the first receive and kept consuming.
    consumers: Pool<Client>,
}

/// A connection a send publishes on, and the queues already declared on it.
struct Publisher {
    client: Client,
    declared: BTreeSet<String>,
}

impl Publisher {
    /// Declare `queue` durable where this connection has not yet, then
    /// publish `bytes` to it persistent, under `key` as its message-id
    /// where there is one, and return once the broker confirms it took
    /// them.
    fn publish(&mut self, queue: &str, bytes: &[u8], key: Option<&str>) -> Result<()> {
        if !self.declared.contains(queue) {
            self.client.declare(queue)?;
            self.declared.insert(queue.to_string());
        }
        let properties = Properties::octets(true).keyed(key);
        self.client.publish_confirmed("", queue, &properties, bytes)
    }
}

impl Pooled for Publisher {
    fn usable(&mut self) -> bool {
        self.client.usable()
    }
}

impl RabbitMqTransport {
    /// Speak to the broker at `broker` about `queue`, as guest until
    /// [`RabbitMqTransport::logging_in`] says otherwise.
    #[must_use]
    pub fn new(broker: impl Into<String>, queue: impl Into<String>) -> Self {
        Self {
            broker: broker.into(),
            queue: queue.into(),
            credentials: Credentials::default(),
            timeout: None,
            publishers: Pool::new(),
            consumers: Pool::new(),
        }
    }

    /// Present these credentials when connecting, and expect them when
    /// accepting.
    #[must_use]
    pub fn logging_in(mut self, credentials: Credentials) -> Self {
        self.credentials = credentials;
        self
    }

    /// Give up on a peer that stops mid-frame, and stop receiving when
    /// the queue has been quiet this long.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Connect to the broker as a client.
    ///
    /// # Errors
    /// Where the broker could not be reached, refused the login, or did
    /// not speak AMQP 0-9-1.
    pub fn connect(&self) -> Result<Client> {
        Client::connect(&self.broker, &self.credentials, self.timeout)
    }

    /// Bind as the far end clients connect to, and report the address.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.broker)
    }

    /// Accept one client on an already-bound listener, expecting this
    /// transport's login.
    ///
    /// # Errors
    /// Where the connection could not be accepted, the handshake failed,
    /// or the client was refused.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Session> {
        Session::accept(listener, &self.credentials, self.timeout)
    }

    /// Where a target names the broker and queue itself —
    /// `rabbitmq://host:5672/orders` — or the broker alone, or is a queue
    /// name alone on this transport's broker.
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, &'a str) {
        match Target::under(&["rabbitmq"], target).map(|named| (named.authority(), named.path())) {
            Some((broker, "")) => (broker, &self.queue),
            Some((broker, queue)) => (broker, queue),
            None => (&self.broker, target),
        }
    }
}

impl Transport for RabbitMqTransport {
    fn name(&self) -> &'static str {
        "rabbitmq"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Ordered(
            "the acknowledgement goes on the session the receive reads from",
        )
    }

    /// Take what is delivered until it has been quiet for the timeout or
    /// the broker closes, on the consumer the first receive declared and
    /// kept: what the broker delivered between two receives waits in the
    /// socket. A quiet queue is an empty vector, not an error. Nothing is
    /// acknowledged here: each delivery's `amqp::acknowledging` answers it
    /// on that consumer after the receive cycle — `basic.ack` when
    /// accepted, `basic.reject` without requeue when refused — `RabbitMQ`
    /// drops it, or dead-letters it where the queue has a dead-letter
    /// exchange — and with requeue when the cycle failed.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let deliveries = self.consumers.exchange(
            self.broker.as_str(),
            || self.connect()?.consuming(&self.queue),
            |client| delivered(client, Client::next_delivery),
        )?;
        Ok(deliveries
            .into_iter()
            .map(|delivery| {
                let origin = format!(
                    "rabbitmq://{}/{}?delivery-tag={}",
                    self.broker, self.queue, delivery.delivery_tag
                );
                let acknowledgement =
                    acknowledging(&self.consumers, &self.broker, delivery.delivery_tag);
                Arrived::whole(origin, delivery.body, acknowledgement)
            })
            .collect())
    }

    /// Declare the queue durable, because the default exchange drops what
    /// it cannot route and says nothing, then publish to it through that
    /// exchange under the queue's own name, persistent and confirmed: on
    /// the connection kept for the broker, and the queue declared on it
    /// once.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        self.publish(target, bytes, None)
    }

    /// The key goes in the message-id property, which a consumer, or the
    /// broker's message deduplication plugin, recognises a repeated
    /// publish by.
    fn send_keyed(&self, target: &str, bytes: &[u8], key: &str) -> Result<()> {
        self.publish(target, bytes, Some(key))
    }
}

impl RabbitMqTransport {
    /// The one send, on the publisher kept for the target's broker.
    fn publish(&self, target: &str, bytes: &[u8], key: Option<&str>) -> Result<()> {
        let (broker, queue) = self.resolve(target);
        self.publishers.exchange(
            broker,
            || {
                Ok(Publisher {
                    client: Client::connect(broker, &self.credentials, self.timeout)?,
                    declared: BTreeSet::new(),
                })
            },
            |publisher| publisher.publish(queue, bytes, key),
        )
    }
}

impl Configured for RabbitMqTransport {
    /// The address is the broker, `host:5672`: where a Location connects.
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[
            Setting {
                name: "queue",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The queue a Receive Location consumes and a Send Location publishes to \
                          when a target names no queue.",
                applies: Applies::Both,
            },
            Setting {
                name: "virtual_host",
                kind: Kind::Text,
                presence: Presence::Optional,
                meaning: "The broker's virtual host the login opens; the default one, `/`, \
                          when left out.",
                applies: Applies::Both,
            },
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long a peer that stops mid-frame is waited on, and how long a \
                          quiet queue ends a receive; unbounded when left out.",
                applies: Applies::Both,
            },
        ],
    };

    /// The user and password come through the Location's credentials, not
    /// a setting; until they are given, the login is the broker's default.
    fn configured(address: &str, settings: &Read) -> Result<Self> {
        let credentials = match settings.optional_text("virtual_host") {
            Some(virtual_host) => Credentials::default().on(virtual_host),
            None => Credentials::default(),
        };
        let transport = Self::new(address, settings.text("queue")).logging_in(credentials);
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
    }
}

impl RabbitMqTransport {
    /// Both ends on this machine: an ephemeral local port, the loopback
    /// timeout, one queue called `probe`, guest at both ends.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0", "probe").timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Accepting for RabbitMqTransport {
    fn take_one(self, listener: &TcpListener) -> Result<Taken> {
        let mut session = self.accept_one(listener)?;
        // The confirm goes out as the publish is read; the client keeps its
        // connection for the next.
        let publish = session
            .next_publish()?
            .ok_or_else(|| protocol_error("the client closed without publishing"))?;
        let origin = format!("rabbitmq://{}/{}", session.peer(), publish.queue());
        Ok(Taken::new(origin, publish.body))
    }
}

impl Loopback for RabbitMqTransport {
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        Ok(Box::new(Listening::new(self.clone(), self.bind()?)))
    }

    /// A client to `address`, the queue declared and the payload published
    /// to it through the default exchange, confirmed before it returns.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        Self {
            broker: address.to_string(),
            ..self.clone()
        }
        .send(&self.queue, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amqp::{Event, Queues};
    use transport::Verdict;
    use transport::payload::{edge_payloads, sized_payloads};

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn far_end(queue: &str) -> RabbitMqTransport {
        RabbitMqTransport::new("127.0.0.1:0", queue)
            .logging_in(Credentials::new("xmip", "secret"))
            .timing_out_after(secs(2))
    }

    #[test]
    fn rabbitmq_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert_eq!(RabbitMqTransport::SETTINGS.problems(), Vec::<String>::new());
        let given = [
            ("queue".to_string(), Given::Text("orders".to_string())),
            ("virtual_host".to_string(), Given::Text("sales".to_string())),
            ("timeout".to_string(), Given::Text("2s".to_string())),
        ];
        let built =
            RabbitMqTransport::open("broker:5672", Applies::Receive, &given).expect("configured");
        assert_eq!(built.queue, "orders");
        assert_eq!(built.credentials.virtual_host, "sales");
        assert_eq!(built.timeout, Some(secs(2)));
        let Err(refused) = RabbitMqTransport::open("broker:5672", Applies::Send, &[]) else {
            panic!("the queue is required");
        };
        assert!(refused.message.contains("\"queue\""), "{refused}");
    }

    #[test]
    fn a_client_publishes_to_a_session_and_the_session_delivers_to_a_receiver() {
        let far_end = far_end("orders");
        let (listener, address) = far_end.bind().expect("binding");
        let long = vec![0x2a; 300_000];
        let sent = long.clone();
        let sender = std::thread::spawn(move || {
            let near = RabbitMqTransport::new(address.clone(), "orders")
                .logging_in(Credentials::new("xmip", "secret"))
                .timing_out_after(secs(2));
            near.send("orders", b"order 1")?;
            near.send(&format!("rabbitmq://{address}"), &sent)?;
            near.send(&format!("rabbitmq://{address}/other"), b"")?;
            near.send("orders", b"order 3")?;
            let receiving = RabbitMqTransport::new(address, "orders")
                .logging_in(Credentials::new("xmip", "secret"))
                .timing_out_after(Duration::from_millis(300));
            let mut arrived = receiving.receive()?.into_iter();
            let one = arrived.next().expect("one").taken()?;
            // Read whole, then refused: the cycle failed after the body.
            let (origin, mut body, acknowledgement) = arrived.next().expect("two").into_parts();
            let mut two = Vec::new();
            std::io::Read::read_to_end(&mut body, &mut two).expect("read");
            acknowledgement.acknowledge(Verdict::Failed)?;
            arrived
                .next()
                .expect("three")
                .refused(transport::Refusal::Forbidden)?;
            Ok::<_, transport::TransportError>((one, Taken::new(origin, two)))
        });
        // One broker, so one connection for all four sends: logged in once.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert_eq!(session.user(), "xmip");
        assert_eq!(session.virtual_host(), "/");
        for expected in [&b"order 1"[..], &long, b"", b"order 3"] {
            let published = session.next_publish().expect("published").expect("one");
            assert_eq!(published.body, expected);
            assert_eq!(published.exchange, "", "the default exchange");
        }
        let queues: Queues = session.into_queues();
        assert_eq!(queues["orders"].len(), 3);
        assert_eq!(queues["other"].len(), 1);
        let mut session = far_end
            .accept_one(&listener)
            .expect("receiver")
            .with_queues(queues);
        let mut events = Vec::new();
        while let Some(event) = session.next_event().expect("serving") {
            events.push(event);
        }
        assert_eq!(events[0], Event::Declared("orders".to_string()));
        assert_eq!(events[1], Event::Consuming("orders".to_string()));
        // Accepted, acked; failed, rejected back onto the queue; refused,
        // rejected for good.
        assert_eq!(events[2], Event::Acked(1));
        assert_eq!(
            events[3],
            Event::Rejected {
                delivery_tag: 2,
                requeue: true
            }
        );
        assert_eq!(
            events[4],
            Event::Rejected {
                delivery_tag: 3,
                requeue: false
            }
        );
        assert_eq!(events.len(), 5);
        assert_eq!(session.queues()["other"].len(), 1, "not consumed");
        let (one, two) = sender.join().expect("thread").expect("receiving");
        assert_eq!(one.bytes, b"order 1");
        assert!(one.origin_uri.ends_with("/orders?delivery-tag=1"));
        assert_eq!(two.bytes, long, "many frames, one body");
        assert!(two.origin_uri.ends_with("/orders?delivery-tag=2"));
    }

    #[test]
    fn a_thousand_sends_log_in_and_declare_once_and_a_closed_connection_is_replaced() {
        const SENDS: usize = 1000;
        let far_end = far_end("orders").timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = RabbitMqTransport::new(address, "orders")
            .logging_in(Credentials::new("xmip", "secret"))
            .timing_out_after(secs(5));
        let sending = near.clone();
        let sender = std::thread::spawn(move || {
            let began = std::time::Instant::now();
            for n in 0..SENDS {
                sending.send("orders", n.to_string().as_bytes())?;
            }
            let took = began.elapsed();
            // Generous for a debug build under load: a millisecond a send.
            assert!(took < Duration::from_millis(SENDS as u64), "{took:?}");
            sending.send("orders", b"after the close")
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        let mut declared = 0;
        let mut published = 0;
        while published < SENDS {
            match session.next_event().expect("serving").expect("one") {
                Event::Declared(_) => declared += 1,
                Event::Published(publish) => {
                    assert_eq!(publish.body, published.to_string().as_bytes());
                    published += 1;
                }
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(declared, 1, "the queue is declared once on a connection");
        drop(session);
        let mut again = far_end.accept_one(&listener).expect("a new connection");
        let last = again.next_publish().expect("publish").expect("one");
        assert_eq!(last.body, b"after the close");
        sender.join().expect("thread").expect("sending");
        assert_eq!(near.publishers.opened(), 2);
    }

    #[test]
    fn five_receives_consume_once_and_a_consumer_the_broker_closed_is_replaced() {
        let far_end = far_end("orders").timing_out_after(Duration::from_secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = RabbitMqTransport::new(address, "orders")
            .logging_in(Credentials::new("xmip", "secret"))
            .timing_out_after(Duration::from_millis(100));
        let receiving = near.clone();
        let (taken, told) = std::sync::mpsc::channel();
        let receiver = std::thread::spawn(move || {
            let mut arrived = Vec::new();
            while arrived.len() < 6 {
                let now = receiving.receive()?;
                if !now.is_empty() {
                    taken.send(()).expect("told");
                }
                for one in now {
                    arrived.push(one.taken()?.bytes);
                }
            }
            Ok::<_, transport::TransportError>(arrived)
        });
        let consuming = |session: &mut Session| {
            let declared = session.next_event().expect("declared");
            assert_eq!(declared, Some(Event::Declared("orders".to_string())));
            let consuming = session.next_event().expect("consuming");
            assert_eq!(consuming, Some(Event::Consuming("orders".to_string())));
        };
        // One login, declaration and consumer for every receive.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        consuming(&mut session);
        for round in 0..5u8 {
            session.deliver("orders", &[round]).expect("delivered");
            told.recv().expect("taken");
        }
        drop(session);
        let mut again = far_end.accept_one(&listener).expect("a new connection");
        consuming(&mut again);
        again.deliver("orders", &[5]).expect("delivered");
        let arrived = receiver.join().expect("thread").expect("receiving");
        assert_eq!(arrived, (0..6u8).map(|n| vec![n]).collect::<Vec<_>>());
        assert_eq!(near.consumers.opened(), 2);
    }

    #[test]
    fn a_refused_login_and_a_broker_that_is_not_amqp_are_permanent() {
        let far_end = far_end("x");
        let (listener, address) = far_end.bind().expect("binding");
        let stranger = std::thread::spawn(move || {
            RabbitMqTransport::new(address, "x")
                .logging_in(Credentials::new("xmip", "wrong"))
                .timing_out_after(secs(2))
                .connect()
                .err()
                .expect("refused")
        });
        let refused = far_end
            .accept_one(&listener)
            .err()
            .expect("refused here too");
        assert!(refused.message.contains("403"), "{refused}");
        let error = stranger.join().expect("thread");
        assert!(!error.retryable, "{error}");
        assert!(error.message.contains("403 ACCESS_REFUSED"), "{error}");

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut header = [0u8; 8];
            std::io::Read::read_exact(&mut stream, &mut header).expect("header");
            std::io::Write::write_all(&mut stream, b"220 mail.example ESMTP\r\n").expect("w");
            // Stay open until the client has read the greeting and gone.
            let _ = std::io::Read::read(&mut stream, &mut header);
        });
        let error = RabbitMqTransport::new(address, "x")
            .timing_out_after(secs(2))
            .connect()
            .err()
            .expect("not amqp");
        assert!(!error.retryable, "{error}");
        let transport = RabbitMqTransport::new("127.0.0.1:0", "x");
        assert!(transport.claims().is_none());
        assert_eq!(transport.name(), "rabbitmq");
        assert!(transport.directions().receives() && transport.directions().sends());
        assert_eq!(transport.resolve("rabbitmq://h:1/a"), ("h:1", "a"));
        assert_eq!(transport.resolve("rabbitmq://h:1"), ("h:1", "x"));
        assert_eq!(transport.resolve("b"), ("127.0.0.1:0", "b"));
    }

    #[test]
    fn the_loopback_round_returns_the_payload_and_its_origin() {
        let loopback = RabbitMqTransport::loopback();
        let arrived = loopback.round(b"published").expect("round");
        assert_eq!(arrived.bytes, b"published");
        assert!(arrived.origin_uri.starts_with("rabbitmq://127.0.0.1:"));
        assert!(arrived.origin_uri.contains("probe"));
        assert!(loopback.ceiling().is_none());
        assert!(loopback.refuses(b"anything").is_none());
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole() {
        let loopback = RabbitMqTransport::loopback();
        for (name, payload) in [edge_payloads(), sized_payloads()].concat() {
            let arrived = loopback.round(&payload).expect(name);
            assert!(arrived.bytes == payload, "{name} came back changed");
        }
    }
}
