use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::AsyncWriteExt;

use bytes::Bytes;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use rustls::{ClientConfig, ServerConfig};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
    time::Instant,
};
use tokio_rustls::TlsStream;
use tokio_util::codec::{Decoder, Framed};
use tracing::{debug, error, info, trace, warn};

use super::frame::{Frame, FrameCodec, FrameError, FrameType};

const DEFAULT_CREDITS: u32 = 16;
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),
    #[error("send error: {0}")]
    Send(#[from] mpsc::error::SendError<Frame>),
    #[error("frame error: {0}")]
    Frame(#[from] FrameError),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("keepalive timeout")]
    KeepaliveTimeout,
    #[error("connection closed")]
    Closed,
}

/// Connection state for credit-based flow control
#[derive(Debug)]
struct FlowControl {
    /// Credits available for sending
    send_credits: u32,
}

impl FlowControl {
    fn new() -> Self { Self { send_credits: DEFAULT_CREDITS } }

    fn has_credit(&self) -> bool { self.send_credits > 0 }

    fn consume_credit(&mut self) {
        if self.send_credits > 0 {
            self.send_credits -= 1;
        }
    }

    fn add_credits(&mut self, credits: u32) { self.send_credits += credits; }

    fn pause(&mut self) { self.send_credits = 0; }
}

pub struct Connection<S> {
    /// The underlying framed stream
    stream: Framed<S, FrameCodec>,
    /// Flow control state
    flow: Arc<Mutex<FlowControl>>,
    /// Last stream ID used
    last_stream: Arc<Mutex<u32>>,
    /// Channel for receiving frames to send
    send_rx: mpsc::Receiver<Frame>,
    /// Channel for sending frames to be sent
    send_tx: mpsc::Sender<Frame>,
    /// Last time we received any frame
    last_received: Instant,
    /// Last time we sent a ping
    last_ping: Option<Instant>,
}

impl<S> Connection<S>
where S: AsyncRead + AsyncWrite + Unpin
{
    /// Create a new connection with the given stream
    pub fn new(stream: S) -> Self {
        debug!("Creating new connection");
        let (send_tx, send_rx) = mpsc::channel(32);
        let conn = Self {
            stream: FrameCodec.framed(stream),
            flow: Arc::new(Mutex::new(FlowControl::new())),
            last_stream: Arc::new(Mutex::new(0)),
            send_rx,
            send_tx,
            last_received: Instant::now(),
            last_ping: None,
        };
        debug!("Connection created successfully");
        conn
    }

    /// Get a sender for sending frames on this connection
    pub fn sender(&self) -> mpsc::Sender<Frame> {
        trace!("Creating new frame sender");
        self.send_tx.clone()
    }

    /// Get the next stream ID if credits are available
    fn next_stream(&self) -> Option<u32> {
        let mut flow = self.flow.lock().unwrap();
        if !flow.has_credit() {
            debug!("No credits available for sending");
            return None;
        }
        flow.consume_credit();
        let mut stream = self.last_stream.lock().unwrap();
        *stream = stream.wrapping_add(1);
        trace!("Generated new stream ID: {}", *stream);
        Some(*stream)
    }

    /// Start a new stream with headers
    pub async fn start_stream(&self, headers: impl Into<Bytes>) -> Result<u32, ConnectionError> {
        let stream_id = self.next_stream().ok_or_else(|| {
            debug!("Failed to get stream ID - no credits available");
            ConnectionError::Protocol("No send credits available".into())
        })?;

        info!("Starting stream {}", stream_id);
        let frame = Frame::header(stream_id, headers);
        debug!("Created header frame: {:?}", frame);

        debug!("Sending header frame to connection");
        if let Err(e) = self.send_tx.send(frame).await {
            error!("Failed to send header frame: {}", e);
            return Err(e.into());
        }
        debug!("Successfully sent header frame");

        Ok(stream_id)
    }

    /// Send body data on an existing stream
    pub async fn send_body(&self, stream_id: u32, data: impl Into<Bytes>) -> Result<(), ConnectionError> {
        debug!("Sending body data on stream {}", stream_id);
        let frame = Frame::body(stream_id, data);
        trace!("Created body frame: {:?}", frame);
        self.send_tx.send(frame).await?;
        debug!("Body data sent successfully");
        Ok(())
    }

    /// End a stream
    pub async fn end_stream(&self, stream_id: u32, trailer: impl Into<Bytes>) -> Result<(), ConnectionError> {
        debug!("Ending stream {}", stream_id);
        let frame = Frame::end(stream_id, trailer);
        trace!("Created end frame: {:?}", frame);
        self.send_tx.send(frame).await?;
        debug!("Stream ended successfully");
        Ok(())
    }

    /// Process an incoming frame
    fn handle_frame(&mut self, frame: Frame) -> Result<(), ConnectionError> {
        debug!("Handling incoming frame: {:?}", frame);
        self.last_received = Instant::now();

        match frame.frame_type {
            FrameType::Credit => {
                if let Some(credits) = frame.get_credits() {
                    debug!("Received {} credits", credits);
                    self.flow.lock().unwrap().add_credits(credits);
                    debug!("Updated flow control credits");
                }
            }
            FrameType::Pause => {
                debug!("Received pause frame - pausing flow control");
                self.flow.lock().unwrap().pause();
            }
            FrameType::Ping => {
                debug!("Received ping frame on stream {}, sending pong", frame.stream);
                let pong = Frame::pong(frame.stream);
                if let Err(e) = self.send_tx.try_send(pong) {
                    warn!("Failed to send pong for ping {}: {}", frame.stream, e);
                } else {
                    debug!("Successfully sent pong for ping {}", frame.stream);
                }
            }
            FrameType::Pong => {
                debug!("Received pong frame, clearing last_ping");
                self.last_ping = None;
            }
            FrameType::Close => {
                info!("Received close frame, initiating connection shutdown");
            }
            _ => {
                debug!("Received frame of type {:?} on stream {}, payload size: {}", frame.frame_type, frame.stream, frame.payload.len());
            }
        }

        Ok(())
    }

    /// Check if we need to send a keepalive ping
    fn check_keepalive(&mut self) -> Result<(), ConnectionError> {
        let now = Instant::now();

        // Check if we've received anything recently
        let since_last_received = now.duration_since(self.last_received);
        if since_last_received > KEEPALIVE_TIMEOUT {
            error!("Keepalive timeout - no messages received for {:?}", since_last_received);
            return Err(ConnectionError::KeepaliveTimeout);
        }

        // Send ping if needed
        match self.last_ping {
            None => {
                if since_last_received > KEEPALIVE_INTERVAL {
                    debug!("Sending keepalive ping after {:?} of inactivity", since_last_received);
                    let ping = Frame::ping(0);
                    if let Err(e) = self.send_tx.try_send(ping) {
                        warn!("Failed to send keepalive ping: {}", e);
                    }
                    self.last_ping = Some(now);
                }
            }
            Some(last_ping) => {
                let ping_duration = now.duration_since(last_ping);
                if ping_duration > KEEPALIVE_TIMEOUT {
                    error!("Keepalive timeout - no pong received for {:?}", ping_duration);
                    return Err(ConnectionError::KeepaliveTimeout);
                }
            }
        }

        Ok(())
    }

    /// Close the connection cleanly
    pub async fn close(mut self) -> Result<(), ConnectionError> {
        info!("Initiating connection close");

        // Send close frame
        debug!("Sending close frame");
        self.stream.send(Frame::close()).await?;
        self.stream.flush().await?;

        // Close the send channel to prevent new frames
        debug!("Dropping send channel");
        drop(self.send_tx);

        // Drain any pending frames
        debug!("Draining pending frames");
        while let Ok(frame) = self.send_rx.try_recv() {
            trace!("Sending pending frame during close: {:?}", frame);
            self.stream.send(frame).await?;
        }

        // Flush any buffered writes
        debug!("Flushing stream");
        self.stream.flush().await?;

        // Close the underlying stream
        debug!("Shutting down stream");
        self.stream.get_mut().shutdown().await?;

        info!("Connection closed successfully");
        Ok(())
    }

    /// Perform the initial handshake as the server
    pub async fn perform_server_handshake(&mut self) -> Result<(), ConnectionError> {
        debug!("Sending handshake frame");
        self.stream.send(Frame::handshake()).await?;
        self.stream.flush().await?;
        debug!("Handshake frame sent and flushed");

        debug!("Waiting for handshake acknowledgment");
        while let Some(frame) = self.stream.next().await {
            match frame {
                Ok(frame) if frame.frame_type == FrameType::HandshakeAck => {
                    debug!("Received handshake acknowledgment");
                    debug!("Sending TlsReady frame");
                    self.stream.send(Frame::tls_ready()).await?;
                    self.stream.flush().await?;
                    debug!("TlsReady frame sent and flushed");
                    return Ok(());
                }
                Ok(frame) => {
                    warn!("Received unexpected frame during handshake: {:?}", frame);
                    continue;
                }
                Err(e) => {
                    error!("Error receiving handshake acknowledgment: {}", e);
                    return Err(e.into());
                }
            }
        }
        error!("Connection closed during handshake");
        Err(ConnectionError::Closed)
    }

    /// Perform the initial handshake as the client
    pub async fn perform_client_handshake(&mut self) -> Result<(), ConnectionError> {
        debug!("Waiting for handshake frame");
        while let Some(frame) = self.stream.next().await {
            match frame {
                Ok(frame) if frame.frame_type == FrameType::Handshake => {
                    debug!("Received handshake frame");
                    break;
                }
                Ok(frame) => {
                    warn!("Received unexpected frame during handshake: {:?}", frame);
                    continue;
                }
                Err(e) => {
                    error!("Error receiving handshake frame: {}", e);
                    return Err(e.into());
                }
            }
        }

        debug!("Sending handshake acknowledgment");
        self.stream.send(Frame::handshake_ack()).await?;
        self.stream.flush().await?;
        debug!("Handshake acknowledgment sent and flushed");

        // Wait for TlsReady frame from server
        debug!("Waiting for TlsReady frame");
        while let Some(frame) = self.stream.next().await {
            match frame {
                Ok(frame) if frame.frame_type == FrameType::TlsReady => {
                    debug!("Received TlsReady frame, handshake complete");
                    return Ok(());
                }
                Ok(frame) => {
                    warn!("Received unexpected frame while waiting for TlsReady: {:?}", frame);
                    continue;
                }
                Err(e) => {
                    error!("Error receiving TlsReady frame: {}", e);
                    return Err(e.into());
                }
            }
        }
        error!("Connection closed while waiting for TlsReady");
        Err(ConnectionError::Closed)
    }
}

impl<S> Stream for Connection<S>
where S: AsyncRead + AsyncWrite + Unpin
{
    type Item = Result<Frame, ConnectionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        debug!("Connection::poll_next called");

        // Check keepalive
        if let Err(e) = this.check_keepalive() {
            error!("Keepalive check failed: {}", e);
            return Poll::Ready(Some(Err(e)));
        }

        // Try to receive a frame first
        debug!("Polling stream for incoming frame");
        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                debug!("Received frame: {:?}", frame);
                if let Err(e) = this.handle_frame(frame.clone()) {
                    error!("Failed to handle frame: {}", e);
                    return Poll::Ready(Some(Err(e)));
                }
                debug!("Successfully handled frame, returning to caller");
                return Poll::Ready(Some(Ok(frame)));
            }
            Poll::Ready(Some(Err(e))) => {
                error!("Error receiving frame: {}", e);
                return Poll::Ready(Some(Err(ConnectionError::Frame(e))));
            }
            Poll::Ready(None) => {
                info!("Stream ended (peer closed connection)");
                return Poll::Ready(None);
            }
            Poll::Pending => {
                debug!("Stream not ready with incoming frame");
            }
        }

        // Then try to send any pending frames
        if let Some(frame) = futures_util::ready!(Pin::new(&mut this.send_rx).poll_recv(cx)) {
            debug!("Attempting to send frame: {:?}", frame);
            match Pin::new(&mut this.stream).poll_ready(cx) {
                Poll::Ready(Ok(())) => {
                    debug!("Stream ready to send frame");
                }
                Poll::Ready(Err(e)) => {
                    error!("Stream error while preparing to send: {}", e);
                    return Poll::Ready(Some(Err(e.into())));
                }
                Poll::Pending => {
                    debug!("Stream not ready to send frame, pending");
                    return Poll::Pending;
                }
            }

            if let Err(e) = Pin::new(&mut this.stream).start_send(frame.clone()) {
                error!("Failed to send frame: {}", e);
                return Poll::Ready(Some(Err(ConnectionError::Frame(e))));
            }
            debug!("Successfully started sending frame: {:?}", frame);

            // Always flush after sending
            match Pin::new(&mut this.stream).poll_flush(cx) {
                Poll::Ready(Ok(())) => {
                    debug!("Successfully flushed stream after sending frame");
                }
                Poll::Ready(Err(e)) => {
                    error!("Failed to flush stream: {}", e);
                    return Poll::Ready(Some(Err(e.into())));
                }
                Poll::Pending => {
                    debug!("Stream flush pending after sending frame");
                    return Poll::Pending;
                }
            }
        }

        // No frame received and nothing to send
        debug!("No frame received and nothing to send, returning Poll::Pending");
        Poll::Pending
    }
}
