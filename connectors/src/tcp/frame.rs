use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

// Framing modeled loosely after BEEP

const MAX_FRAME_SIZE: usize = 1024 * 1024 * 16; // 16MB max frame size
const HEADER_SIZE: usize = 9; // 4 bytes msgno + 1 byte type + 4 bytes length

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("frame too large: {size} bytes")]
    FrameTooLarge { size: usize },
    #[error("incomplete frame")]
    Incomplete,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid frame type: {0}")]
    InvalidFrameType(u8),
}

/// Frame types for basic request/response and push patterns
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    /// Push message (no response expected)
    Push = 0,
    /// Request message (response expected)
    Req = 1,
    /// Response to a request
    Res = 2,
    /// Error response
    Err = 3,
    /// Keepalive ping
    Ping = 4,
    /// Keepalive pong
    Pong = 5,
    /// Grant more send credits
    Credit = 6,
    /// Pause sending (credit = 0)
    Pause = 7,
    /// Close the connection
    Close = 8,
    /// Server signals TLS setup is complete
    TlsReady = 9,
    /// Initial protocol handshake
    Handshake = 10,
    /// Handshake acknowledgment
    HandshakeAck = 11,
}

impl TryFrom<u8> for FrameType {
    type Error = FrameError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(FrameType::Push),
            1 => Ok(FrameType::Req),
            2 => Ok(FrameType::Res),
            3 => Ok(FrameType::Err),
            4 => Ok(FrameType::Ping),
            5 => Ok(FrameType::Pong),
            6 => Ok(FrameType::Credit),
            7 => Ok(FrameType::Pause),
            8 => Ok(FrameType::Close),
            9 => Ok(FrameType::TlsReady),
            10 => Ok(FrameType::Handshake),
            11 => Ok(FrameType::HandshakeAck),
            n => Err(FrameError::InvalidFrameType(n)),
        }
    }
}

/// A frame in our protocol
#[derive(Debug, Clone)]
pub struct Frame {
    /// Message number for request/response matching
    /// - For Push: 0
    /// - For Req: new sequence number
    /// - For Res/Err: sequence number of the request
    pub msgno: u32,
    /// Frame type
    pub frame_type: FrameType,
    /// Payload data
    pub payload: Bytes,
}

impl Frame {
    /// Create a new frame
    pub fn new(frame_type: FrameType, msgno: u32, payload: impl Into<Bytes>) -> Self { Self { frame_type, msgno, payload: payload.into() } }

    /// Create a push message (no response expected)
    pub fn push(payload: impl Into<Bytes>) -> Self { Self::new(FrameType::Push, 0, payload) }

    /// Create a request message
    pub fn request(msgno: u32, payload: impl Into<Bytes>) -> Self { Self::new(FrameType::Req, msgno, payload) }

    /// Create a response message
    pub fn response(msgno: u32, payload: impl Into<Bytes>) -> Self { Self::new(FrameType::Res, msgno, payload) }

    /// Create an error response
    pub fn error(msgno: u32, payload: impl Into<Bytes>) -> Self { Self::new(FrameType::Err, msgno, payload) }

    /// Create a ping message
    pub fn ping(msgno: u32) -> Self { Self::new(FrameType::Ping, msgno, Bytes::new()) }

    /// Create a pong message (response to ping)
    pub fn pong(msgno: u32) -> Self { Self::new(FrameType::Pong, msgno, Bytes::new()) }

    /// Create a credit frame granting more send credits
    /// The payload contains the number of additional messages that can be sent
    pub fn credit(credits: u32) -> Self {
        let mut buf = BytesMut::with_capacity(4);
        buf.put_u32(credits);
        Self::new(FrameType::Credit, 0, buf.freeze())
    }

    /// Create a pause frame requesting the sender to stop
    pub fn pause() -> Self { Self::new(FrameType::Pause, 0, Bytes::new()) }

    /// Get the number of credits from a Credit frame
    pub fn get_credits(&self) -> Option<u32> {
        if self.frame_type == FrameType::Credit && self.payload.len() >= 4 { Some((&self.payload[..4]).get_u32()) } else { None }
    }

    /// Create a close frame to signal connection closure
    pub fn close() -> Self { Self::new(FrameType::Close, 0, Bytes::new()) }

    /// Create a handshake frame
    pub fn handshake() -> Self { Self::new(FrameType::Handshake, 0, Bytes::new()) }

    /// Create a handshake acknowledgment frame
    pub fn handshake_ack() -> Self { Self::new(FrameType::HandshakeAck, 0, Bytes::new()) }

    /// Create a TLS ready frame
    pub fn tls_ready() -> Self { Self::new(FrameType::TlsReady, 0, Bytes::new()) }
}

/// Codec for encoding/decoding frames
pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = FrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least a header
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }

        // Read frame header without consuming
        let msgno = (&src[..4]).get_u32();
        let frame_type = FrameType::try_from(src[4])?;
        let len = (&src[5..9]).get_u32() as usize;

        // Validate frame size
        if len > MAX_FRAME_SIZE {
            return Err(FrameError::FrameTooLarge { size: len });
        }

        // Check if we have the full frame
        if src.len() < HEADER_SIZE + len {
            return Ok(None);
        }

        // Now we can consume the header
        src.advance(HEADER_SIZE);

        // Extract payload
        let payload = src.split_to(len).freeze();

        Ok(Some(Frame { msgno, frame_type, payload }))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = FrameError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload_len = item.payload.len();
        if payload_len > MAX_FRAME_SIZE {
            return Err(FrameError::FrameTooLarge { size: payload_len });
        }

        // Reserve space for header + payload
        dst.reserve(HEADER_SIZE + payload_len);

        // Write header
        dst.put_u32(item.msgno);
        dst.put_u8(item.frame_type as u8);
        dst.put_u32(payload_len as u32);

        // Write payload
        dst.extend_from_slice(&item.payload);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_type_conversion() {
        // Valid conversions
        assert_eq!(FrameType::try_from(0).unwrap(), FrameType::Push);
        assert_eq!(FrameType::try_from(1).unwrap(), FrameType::Req);
        assert_eq!(FrameType::try_from(2).unwrap(), FrameType::Res);
        assert_eq!(FrameType::try_from(4).unwrap(), FrameType::Ping);
        assert_eq!(FrameType::try_from(5).unwrap(), FrameType::Pong);
        assert_eq!(FrameType::try_from(8).unwrap(), FrameType::Close);

        // Invalid conversion
        assert!(matches!(FrameType::try_from(9), Err(FrameError::InvalidFrameType(9))));
    }

    #[test]
    fn test_frame_construction() {
        let push = Frame::push(Vec::from(&b"event"[..]));
        assert_eq!(push.frame_type, FrameType::Push);
        assert_eq!(push.msgno, 0);
        assert_eq!(&push.payload[..], b"event");

        let req = Frame::request(42, Vec::from(&b"request"[..]));
        assert_eq!(req.frame_type, FrameType::Req);
        assert_eq!(req.msgno, 42);
        assert_eq!(&req.payload[..], b"request");

        let res = Frame::response(42, Vec::from(&b"response"[..]));
        assert_eq!(res.frame_type, FrameType::Res);
        assert_eq!(res.msgno, 42);
        assert_eq!(&res.payload[..], b"response");
    }

    #[test]
    fn test_encode_decode() {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::new();

        // Create and encode a frame
        let frame = Frame::request(42, Vec::from(&b"test payload"[..]));
        codec.encode(frame, &mut buf).unwrap();

        // Verify encoded size
        assert_eq!(buf.len(), HEADER_SIZE + b"test payload".len());

        // Decode it back
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.frame_type, FrameType::Req);
        assert_eq!(decoded.msgno, 42);
        assert_eq!(&decoded.payload[..], b"test payload");

        // Buffer should be empty now
        assert!(buf.is_empty());
    }

    #[test]
    fn test_push_encode_decode() {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::new();

        // Create and encode a push frame
        let frame = Frame::push(Vec::from(&b"event data"[..]));
        codec.encode(frame, &mut buf).unwrap();

        // Decode and verify
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.frame_type, FrameType::Push);
        assert_eq!(decoded.msgno, 0);
        assert_eq!(&decoded.payload[..], b"event data");
    }

    #[test]
    fn test_request_response() {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::new();

        // Request
        let request = Frame::request(1, Vec::from(&b"request"[..]));
        codec.encode(request, &mut buf).unwrap();

        // Response
        let response = Frame::response(1, Vec::from(&b"response"[..]));
        codec.encode(response, &mut buf).unwrap();

        // Decode request
        let decoded_req = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded_req.frame_type, FrameType::Req);
        assert_eq!(decoded_req.msgno, 1);
        assert_eq!(&decoded_req.payload[..], b"request");

        // Decode response
        let decoded_res = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded_res.frame_type, FrameType::Res);
        assert_eq!(decoded_res.msgno, 1); // Same as request
        assert_eq!(&decoded_res.payload[..], b"response");
    }

    #[test]
    fn test_keepalive() {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::new();

        // Ping with sequence 1
        let ping = Frame::ping(1);
        codec.encode(ping, &mut buf).unwrap();

        // Pong response with same sequence
        let pong = Frame::pong(1);
        codec.encode(pong, &mut buf).unwrap();

        // Decode ping
        let decoded_ping = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded_ping.frame_type, FrameType::Ping);
        assert_eq!(decoded_ping.msgno, 1);
        assert!(decoded_ping.payload.is_empty());

        // Decode pong
        let decoded_pong = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded_pong.frame_type, FrameType::Pong);
        assert_eq!(decoded_pong.msgno, 1); // Same as ping
        assert!(decoded_pong.payload.is_empty());
    }

    #[test]
    fn test_flow_control() {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::new();

        // Grant 10 credits
        let credit = Frame::credit(10);
        codec.encode(credit, &mut buf).unwrap();

        // Send pause
        let pause = Frame::pause();
        codec.encode(pause, &mut buf).unwrap();

        // Decode credit frame
        let decoded_credit = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded_credit.frame_type, FrameType::Credit);
        assert_eq!(decoded_credit.get_credits(), Some(10));

        // Decode pause frame
        let decoded_pause = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded_pause.frame_type, FrameType::Pause);
        assert!(decoded_pause.payload.is_empty());
    }

    #[test]
    fn test_credit_decode() {
        // Test valid credit frame
        let frame = Frame::credit(42);
        assert_eq!(frame.get_credits(), Some(42));

        // Test non-credit frame
        let frame = Frame::push(Bytes::new());
        assert_eq!(frame.get_credits(), None);
    }

    #[test]
    fn test_partial_decode() {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::new();

        // Encode a frame
        let frame = Frame::request(1, Vec::from(&b"test"[..]));
        codec.encode(frame, &mut buf).unwrap();

        // Test with partial header (just msgno)
        let mut partial_buf = BytesMut::from(&buf[..4]);
        assert!(codec.decode(&mut partial_buf).unwrap().is_none());

        // Test with full header but partial payload
        let mut partial_buf = BytesMut::from(&buf[..HEADER_SIZE + 2]);
        assert!(codec.decode(&mut partial_buf).unwrap().is_none());

        // Test with complete frame
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.frame_type, FrameType::Req);
        assert_eq!(decoded.msgno, 1);
        assert_eq!(&decoded.payload[..], b"test");
    }

    #[test]
    fn test_max_size_limits() {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::new();

        // Try to encode a frame that's too large
        let large_payload = vec![0u8; MAX_FRAME_SIZE + 1];
        let frame = Frame::request(1, large_payload);
        assert!(matches!(
            codec.encode(frame, &mut buf),
            Err(FrameError::FrameTooLarge { size }) if size > MAX_FRAME_SIZE
        ));

        // Try to decode a frame that claims to be too large
        buf.put_u32(1); // msgno
        buf.put_u8(FrameType::Push as u8);
        buf.put_u32((MAX_FRAME_SIZE + 1) as u32);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(FrameError::FrameTooLarge { size }) if size > MAX_FRAME_SIZE
        ));
    }

    #[test]
    fn test_malformed_credit_frame() {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::new();

        // Create a credit frame with truncated payload
        buf.put_u32(0); // msgno
        buf.put_u8(FrameType::Credit as u8);
        buf.put_u32(2); // payload length of 2 (too short for credits)
        buf.put_u16(0); // incomplete credit value

        // Decode should succeed but get_credits should return None
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.get_credits(), None);
    }

    #[test]
    fn test_zero_length_frames() {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::new();

        // Test various frame types with empty payloads
        let frames = vec![
            Frame::push(Bytes::new()),
            Frame::request(1, Bytes::new()),
            Frame::response(1, Bytes::new()),
            Frame::error(1, Bytes::new()),
            Frame::ping(1),
            Frame::pong(1),
            Frame::pause(),
        ];

        // Encode all frames
        for frame in frames.iter() {
            codec.encode(frame.clone(), &mut buf).unwrap();
        }

        // Decode and verify
        for expected in frames {
            let decoded = codec.decode(&mut buf).unwrap().unwrap();
            assert_eq!(decoded.frame_type, expected.frame_type);
            assert_eq!(decoded.msgno, expected.msgno);
            assert!(decoded.payload.is_empty());
        }
    }

    #[test]
    fn test_message_number_boundaries() {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::new();

        // Test boundary values for message numbers
        let test_cases = vec![
            0u32,         // Min value
            1,            // Common case
            u32::MAX - 1, // Near max
            u32::MAX,     // Max value
        ];

        // Encode all frames
        for &msgno in &test_cases {
            // Create and encode a request
            let request = Frame::request(msgno, Vec::from(&b"test"[..]));
            codec.encode(request, &mut buf).unwrap();

            // Create and encode a response
            let response = Frame::response(msgno, Vec::from(&b"test"[..]));
            codec.encode(response, &mut buf).unwrap();
        }

        // Decode and verify all frames
        for &msgno in &test_cases {
            // Verify request
            let decoded = codec.decode(&mut buf).unwrap().unwrap();
            assert_eq!(decoded.frame_type, FrameType::Req);
            assert_eq!(decoded.msgno, msgno);

            // Verify response
            let decoded = codec.decode(&mut buf).unwrap().unwrap();
            assert_eq!(decoded.frame_type, FrameType::Res);
            assert_eq!(decoded.msgno, msgno);
        }
    }
}
