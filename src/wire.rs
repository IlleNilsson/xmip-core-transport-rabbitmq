//! AMQP 0-9-1 on the wire: the frame envelope, and the value encodings
//! every method argument is built from.
//!
//! A frame is a type octet, a channel short, a length long, that many
//! bytes of payload, and the frame-end octet `0xCE`. Everything inside is
//! big-endian. Strings come in two lengths — a short one counted by an
//! octet, a long one counted by a long — and a field table is a long
//! length followed by short-string keys each tagged with the type of the
//! value that follows.
//!
//! Nothing here knows what a method means; that is `method.rs`.

use std::io::{BufRead, Read};

use transport::error::{Result, classify, protocol_error};

/// The octet every frame ends with.
pub const FRAME_END: u8 = 0xCE;

/// The largest frame Xmip will read, and what it asks for in tune-ok.
pub const MAX_FRAME: usize = 1024 * 1024;

/// The protocol header a connection opens with: `AMQP`, then 0, 0, 9, 1.
pub const PROTOCOL_HEADER: &[u8] = b"AMQP\x00\x00\x09\x01";

/// What a frame carries, by its type octet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    /// A class and method id, and the arguments they define. Type 1.
    Method,
    /// A class, a body size and the properties. Type 2.
    Header,
    /// Part of a message body. Type 3.
    Body,
    /// A heartbeat. Type 8.
    Heartbeat,
}

impl Kind {
    /// The type octet on the wire.
    #[must_use]
    pub const fn octet(self) -> u8 {
        match self {
            Self::Method => 1,
            Self::Header => 2,
            Self::Body => 3,
            Self::Heartbeat => 8,
        }
    }

    /// What `octet` says the frame carries.
    #[must_use]
    pub const fn of(octet: u8) -> Option<Self> {
        match octet {
            1 => Some(Self::Method),
            2 => Some(Self::Header),
            3 => Some(Self::Body),
            8 => Some(Self::Heartbeat),
            _ => None,
        }
    }
}

/// One frame: what it carries, on which channel, and its payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    pub kind: Kind,
    pub channel: u16,
    pub payload: Vec<u8>,
}

impl Frame {
    /// A frame of `kind` on `channel`.
    #[must_use]
    pub fn new(kind: Kind, channel: u16, payload: Vec<u8>) -> Self {
        Self {
            kind,
            channel,
            payload,
        }
    }

    /// The frame as bytes, frame-end included.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.payload.len() + 8);
        out.push(self.kind.octet());
        put_short(&mut out, self.channel);
        put_long(&mut out, len32(self.payload.len()));
        out.extend_from_slice(&self.payload);
        out.push(FRAME_END);
        out
    }
}

/// Read one frame, or `None` when the peer closed between frames.
///
/// # Errors
/// A type octet AMQP does not define, a frame over [`MAX_FRAME`], a frame
/// that does not end in `0xCE`, or a connection that broke.
pub fn read_frame(reader: &mut impl BufRead) -> Result<Option<Frame>> {
    let mut head = [0u8; 7];
    match reader.read_exact(&mut head) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(classify("reading a frame header", &error)),
    }
    let kind = Kind::of(head[0])
        .ok_or_else(|| protocol_error(format!("a frame type AMQP does not define: {}", head[0])))?;
    let channel = u16::from_be_bytes([head[1], head[2]]);
    let size = u32::from_be_bytes([head[3], head[4], head[5], head[6]]) as usize;
    if size > MAX_FRAME {
        return Err(protocol_error(format!(
            "a frame of {size} bytes, over the {MAX_FRAME} byte limit"
        )));
    }
    let mut payload = vec![0u8; size + 1];
    reader
        .read_exact(&mut payload)
        .map_err(|e| classify("reading a frame payload", &e))?;
    if payload.pop() != Some(FRAME_END) {
        return Err(protocol_error("a frame that does not end in 0xCE"));
    }
    Ok(Some(Frame {
        kind,
        channel,
        payload,
    }))
}

/// A length as the wire writes it, saturating rather than wrapping: a
/// payload longer than a `u32` cannot be framed and is refused where it is
/// built.
#[must_use]
pub fn len32(length: usize) -> u32 {
    u32::try_from(length).unwrap_or(u32::MAX)
}

/// Append one octet.
pub fn put_octet(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

/// Append a short: two octets, big-endian.
pub fn put_short(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Append a long: four octets, big-endian.
pub fn put_long(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Append a long long: eight octets, big-endian.
pub fn put_longlong(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Append a short string: an octet of length, then the bytes. Longer than
/// 255 bytes is truncated, which only a malformed name could be.
pub fn put_short_string(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    let length = u8::try_from(bytes.len()).unwrap_or(u8::MAX) as usize;
    out.push(u8::try_from(length).unwrap_or(u8::MAX));
    out.extend_from_slice(&bytes[..length]);
}

/// Append a long string: a long of length, then the bytes.
pub fn put_long_string(out: &mut Vec<u8>, value: &[u8]) {
    put_long(out, len32(value.len()));
    out.extend_from_slice(value);
}

/// Append an empty field table — a long zero. The tables Xmip sends carry
/// nothing; [`table`] reads the ones a broker sends.
pub fn put_empty_table(out: &mut Vec<u8>) {
    put_long(out, 0);
}

/// Reading a payload one value at a time, in the order the method defines.
pub struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    /// Read `bytes` from the start.
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    /// What has not been read yet.
    #[must_use]
    pub fn rest(&self) -> &'a [u8] {
        &self.bytes[self.at.min(self.bytes.len())..]
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| protocol_error("a method that ends before its arguments do"))?;
        let taken = &self.bytes[self.at..end];
        self.at = end;
        Ok(taken)
    }

    /// One octet.
    ///
    /// # Errors
    /// Where the payload has run out.
    pub fn octet(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    /// Two octets, big-endian.
    ///
    /// # Errors
    /// Where the payload has run out.
    pub fn short(&mut self) -> Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    /// Four octets, big-endian.
    ///
    /// # Errors
    /// Where the payload has run out.
    pub fn long(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Eight octets, big-endian.
    ///
    /// # Errors
    /// Where the payload has run out.
    pub fn longlong(&mut self) -> Result<u64> {
        let bytes = self.take(8)?;
        let mut eight = [0u8; 8];
        eight.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(eight))
    }

    /// A short string, as text.
    ///
    /// # Errors
    /// Where the payload has run out or the bytes are not UTF-8.
    pub fn short_string(&mut self) -> Result<String> {
        let length = self.octet()? as usize;
        text(self.take(length)?)
    }

    /// A long string, as bytes.
    ///
    /// # Errors
    /// Where the payload has run out.
    pub fn long_string(&mut self) -> Result<Vec<u8>> {
        let length = self.long()? as usize;
        Ok(self.take(length)?.to_vec())
    }

    /// A field table, skipped: Xmip reads none of what a broker puts in
    /// its server properties, and skipping it is the whole of the need.
    ///
    /// # Errors
    /// Where the payload has run out.
    pub fn skip_table(&mut self) -> Result<()> {
        let length = self.long()? as usize;
        self.take(length).map(|_| ())
    }
}

fn text(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec()).map_err(|_| protocol_error("a string that is not UTF-8"))
}

/// A field table's common types, read as text so a diagnostic can print
/// them: `S` long string, `I` long int, `t` boolean, `F` nested table, `V`
/// nothing. Anything else ends the reading, because the length of an
/// unknown type is unknown.
///
/// # Errors
/// Where the table ends mid-value.
pub fn table(bytes: &[u8]) -> Result<Vec<(String, String)>> {
    let mut reader = Reader::new(bytes);
    let mut pairs = Vec::new();
    while !reader.rest().is_empty() {
        let key = reader.short_string()?;
        let value = match reader.octet()? {
            b'S' => text(&reader.long_string()?)?,
            b'I' => (reader.long()? as i32).to_string(),
            b't' => (reader.octet()? != 0).to_string(),
            b'F' => {
                let nested = reader.long_string()?;
                format!("{:?}", table(&nested)?)
            }
            b'V' => String::new(),
            _ => break,
        };
        pairs.push((key, value));
    }
    Ok(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frame_round_trips_through_its_envelope() {
        let frame = Frame::new(Kind::Method, 1, vec![0, 10, 0, 11]);
        let bytes = frame.encode();
        assert_eq!(bytes[0], 1);
        assert_eq!(&bytes[1..3], &[0, 1]);
        assert_eq!(&bytes[3..7], &[0, 0, 0, 4]);
        assert_eq!(*bytes.last().expect("frame end"), FRAME_END);
        let back = read_frame(&mut bytes.as_slice()).expect("read").expect("one");
        assert_eq!(back, frame);
        assert!(read_frame(&mut &b""[..]).expect("closed").is_none());
        assert_eq!(Kind::of(8), Some(Kind::Heartbeat));
        assert_eq!(Kind::Body.octet(), 3);
    }

    #[test]
    fn what_is_not_amqp_is_refused() {
        assert!(read_frame(&mut &b"AMQP\x00\x00\x09\x01"[..]).is_err(), "type");
        let mut bad_end = Frame::new(Kind::Body, 1, vec![7]).encode();
        *bad_end.last_mut().expect("last") = 0;
        assert!(read_frame(&mut bad_end.as_slice()).is_err(), "frame end");
        let huge = [3u8, 0, 1, 0xFF, 0xFF, 0xFF, 0xFF];
        assert!(read_frame(&mut &huge[..]).is_err(), "over the limit");
        let short = [1u8, 0, 1, 0, 0, 0, 4, 0];
        assert!(read_frame(&mut &short[..]).is_err(), "ends mid-payload");
    }

    #[test]
    fn every_value_encoding_round_trips() {
        let mut out = Vec::new();
        put_octet(&mut out, 7);
        put_short(&mut out, 4096);
        put_long(&mut out, 131_072);
        put_longlong(&mut out, u64::MAX);
        put_short_string(&mut out, "orders");
        put_long_string(&mut out, b"a longer one");
        put_empty_table(&mut out);
        let mut reader = Reader::new(&out);
        assert_eq!(reader.octet().expect("octet"), 7);
        assert_eq!(reader.short().expect("short"), 4096);
        assert_eq!(reader.long().expect("long"), 131_072);
        assert_eq!(reader.longlong().expect("longlong"), u64::MAX);
        assert_eq!(reader.short_string().expect("short string"), "orders");
        assert_eq!(reader.long_string().expect("long string"), b"a longer one");
        reader.skip_table().expect("table");
        assert!(reader.rest().is_empty());
        assert!(reader.octet().is_err(), "nothing left");
        assert_eq!(len32(usize::MAX), u32::MAX);
    }

    #[test]
    fn a_field_table_reads_the_types_a_broker_sends() {
        let mut inner = Vec::new();
        put_short_string(&mut inner, "publish");
        inner.push(b't');
        inner.push(1);
        let mut out = Vec::new();
        put_short_string(&mut out, "product");
        out.push(b'S');
        put_long_string(&mut out, b"RabbitMQ");
        put_short_string(&mut out, "channel_max");
        out.push(b'I');
        put_long(&mut out, 2047);
        put_short_string(&mut out, "capabilities");
        out.push(b'F');
        put_long_string(&mut out, &inner);
        put_short_string(&mut out, "nothing");
        out.push(b'V');
        let pairs = table(&out).expect("table");
        assert_eq!(pairs[0], ("product".into(), "RabbitMQ".into()));
        assert_eq!(pairs[1], ("channel_max".into(), "2047".into()));
        assert!(pairs[2].1.contains("publish"));
        assert_eq!(pairs[3], ("nothing".into(), String::new()));
        let mut unknown = Vec::new();
        put_short_string(&mut unknown, "x");
        unknown.push(b'?');
        assert!(table(&unknown).expect("stops").is_empty());
    }
}
