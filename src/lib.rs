#![forbid(unsafe_code)]

//! Streams that arrive as deliveries from a `RabbitMQ` queue. One
//! basic.deliver is one Stream, the queue kept beside it.
//!
//! `RabbitMQ` listens for AMQP 0-9-1 on port 5672, and a queue is the
//! Location: a Receive Location connects with PLAIN, declares its queue
//! durable, consumes it and acknowledges each delivery once it is a Stream;
//! a Send Location declares the queue and publishes to it through the
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

use std::net::TcpListener;
use std::time::Duration;

use amqp::{Client, Login, Session};
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, Transport};

#[derive(Clone)]
pub struct RabbitMqTransport {
    broker: String,
    queue: String,
    login: Login,
    timeout: Option<Duration>,
}

impl RabbitMqTransport {
    /// Speak to the broker at `broker` about `queue`, as guest until
    /// [`RabbitMqTransport::logging_in`] says otherwise.
    #[must_use]
    pub fn new(broker: impl Into<String>, queue: impl Into<String>) -> Self {
        Self {
            broker: broker.into(),
            queue: queue.into(),
            login: Login::default(),
            timeout: None,
        }
    }

    /// Present this login when connecting, and expect it when accepting.
    #[must_use]
    pub fn logging_in(mut self, login: Login) -> Self {
        self.login = login;
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
        Client::connect(&self.broker, &self.login, self.timeout)
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
        Session::accept(listener, &self.login, self.timeout)
    }

    /// Where a target names the broker and queue itself —
    /// `rabbitmq://host:5672/orders` — or the broker alone, or is a queue
    /// name alone on this transport's broker.
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, &'a str) {
        match socket::target("rabbitmq", target) {
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

    /// Declare and consume the queue and take what is delivered, each
    /// acknowledged, until it has been quiet for the timeout or the broker
    /// closes. A quiet queue is an empty vector, not an error.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let mut client = self.connect()?;
        let deliveries = client.drain(&self.queue)?;
        client.close()?;
        Ok(deliveries
            .into_iter()
            .map(|delivery| {
                let origin = format!(
                    "rabbitmq://{}/{}?delivery-tag={}",
                    self.broker, self.queue, delivery.delivery_tag
                );
                Arrived::new(origin, delivery.body)
            })
            .collect())
    }

    /// Declare the queue durable, because the default exchange drops what
    /// it cannot route and says nothing, then publish to it through that
    /// exchange under the queue's own name, persistent.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (broker, queue) = self.resolve(target);
        let mut client = Client::connect(broker, &self.login, self.timeout)?;
        client.declare(queue)?;
        client.publish("", queue, bytes, true)?;
        client.close()
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
    fn take_one(self, listener: &TcpListener) -> Result<Arrived> {
        let mut session = self.accept_one(listener)?;
        let publish = session
            .next_publish()?
            .ok_or_else(|| protocol_error("the client closed without publishing"))?;
        // The client closes the channel and the connection and waits for
        // each -ok; serve them, and see the client go.
        session.next_publish()?;
        let origin = format!("rabbitmq://{}/{}", session.peer(), publish.queue());
        Ok(Arrived::new(origin, publish.body))
    }
}

impl Loopback for RabbitMqTransport {
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        Ok(Box::new(Listening::new(self.clone(), self.bind()?)))
    }

    /// A fresh client to `address`, the queue declared and the payload
    /// published to it through the default exchange, closed before it
    /// returns.
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
    use transport::payload::{edge_payloads, sized_payloads};

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn far_end(queue: &str) -> RabbitMqTransport {
        RabbitMqTransport::new("127.0.0.1:0", queue)
            .logging_in(Login::new("xmip", "secret"))
            .timing_out_after(secs(2))
    }

    #[test]
    fn a_client_publishes_to_a_session_and_the_session_delivers_to_a_receiver() {
        let far_end = far_end("orders");
        let (listener, address) = far_end.bind().expect("binding");
        let long = vec![0x2a; 300_000];
        let sent = long.clone();
        let sender = std::thread::spawn(move || {
            let near = RabbitMqTransport::new(address.clone(), "orders")
                .logging_in(Login::new("xmip", "secret"))
                .timing_out_after(secs(2));
            near.send("orders", b"order 1")?;
            near.send(&format!("rabbitmq://{address}"), &sent)?;
            near.send(&format!("rabbitmq://{address}/other"), b"")?;
            RabbitMqTransport::new(address, "orders")
                .logging_in(Login::new("xmip", "secret"))
                .timing_out_after(Duration::from_millis(300))
                .receive()
        });
        let mut queues = Queues::new();
        for expected in [&b"order 1"[..], &long, b""] {
            let mut session = far_end
                .accept_one(&listener)
                .expect("accepting")
                .with_queues(queues);
            assert_eq!(session.user(), "xmip");
            assert_eq!(session.virtual_host(), "/");
            let published = session.next_publish().expect("published").expect("one");
            assert_eq!(published.body, expected);
            assert_eq!(published.exchange, "", "the default exchange");
            assert!(session.next_publish().expect("closed").is_none());
            queues = session.into_queues();
        }
        assert_eq!(queues["orders"].len(), 2);
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
        assert_eq!(events[2], Event::Acked(1));
        assert_eq!(events[3], Event::Acked(2));
        assert_eq!(events.len(), 4);
        assert_eq!(session.queues()["other"].len(), 1, "not consumed");
        let arrived = sender.join().expect("thread").expect("receiving");
        assert_eq!(arrived.len(), 2);
        assert_eq!(arrived[0].bytes, b"order 1");
        assert!(arrived[0].origin_uri.ends_with("/orders?delivery-tag=1"));
        assert_eq!(arrived[1].bytes, long, "many frames, one body");
        assert!(arrived[1].origin_uri.ends_with("/orders?delivery-tag=2"));
    }

    #[test]
    fn a_refused_login_and_a_broker_that_is_not_amqp_are_permanent() {
        let far_end = far_end("x");
        let (listener, address) = far_end.bind().expect("binding");
        let stranger = std::thread::spawn(move || {
            RabbitMqTransport::new(address, "x")
                .logging_in(Login::new("xmip", "wrong"))
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
