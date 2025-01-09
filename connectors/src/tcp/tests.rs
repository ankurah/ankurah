#[tokio::test]
async fn test_ping_pong() {
    use super::{Client, Frame, FrameType, Listener, tls::Cert};
    use futures_util::StreamExt;
    use rustls::ServerConfig;
    use std::sync::Arc;
    use tracing::{debug, info};

    let _ = tracing_subscriber::fmt::try_init();

    // Create test certificates
    let cert = Cert::self_signed(["localhost"]).unwrap();
    let server_config = ServerConfig::builder().with_no_client_auth().with_single_cert(vec![cert.cert], cert.key).unwrap();
    let server_config = Arc::new(server_config);

    // Start server
    let listener = Listener::bind(server_config, "127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    info!("Server listening on {}", addr);

    let server = tokio::spawn(async move {
        info!("Server waiting for connection");
        let mut conn = listener.accept().await.unwrap();
        info!("Server accepted connection");

        // Handle one frame then close
        let frame = conn.next().await.unwrap().unwrap();
        debug!("Server received frame: {:?}", frame);
        info!("Server received ping, sending pong");

        // Send pong before closing
        conn.sender().send(Frame::pong(frame.msgno)).await.unwrap();
        info!("Server sent pong, initiating connection close");

        // Send close frame and close connection
        conn.sender().send(Frame::close()).await.unwrap();
        conn.close().await.unwrap();
    });

    // Connect client
    let client = Client::new_insecure("localhost", addr.to_string()).unwrap();
    let mut conn = client.connect().await.unwrap();
    info!("Client connected");

    // Send ping and wait for pong
    info!("Client sending ping");
    conn.sender().send(Frame::ping(1)).await.unwrap();
    debug!("Client polling after ping");

    // Wait for pong and close frame
    while let Some(frame) = conn.next().await {
        let frame = frame.unwrap();
        match frame.frame_type {
            FrameType::Pong => {
                info!("Client received pong");
            }
            FrameType::Close => {
                info!("Client received close frame, closing connection");
                conn.close().await.unwrap();
                break;
            }
            _ => {
                debug!("Client received unexpected frame: {:?}", frame);
            }
        }
    }

    server.await.unwrap();
}

#[tokio::test]
async fn test_request() {
    use super::{Client, Frame, FrameType, Listener, tls::Cert};
    use futures_util::StreamExt;
    use rustls::ServerConfig;
    use std::sync::Arc;
    use tracing::{debug, info};

    let _ = tracing_subscriber::fmt::try_init();

    // Create test certificates
    let cert = Cert::self_signed(["localhost"]).unwrap();
    let server_config = ServerConfig::builder().with_no_client_auth().with_single_cert(vec![cert.cert], cert.key).unwrap();

    // Start server
    let listener = Listener::bind(Arc::new(server_config), "127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    info!("Server listening on {}", server_addr);

    // Spawn server handler
    let server_handle = tokio::spawn(async move {
        info!("Server waiting for connection");
        let mut conn = listener.accept().await.unwrap();
        info!("Server accepted connection");

        // Process frames until we get a Close frame
        debug!("Server starting frame processing loop");
        while let Some(frame_result) = conn.next().await {
            debug!("Server received frame result: {:?}", frame_result);
            let frame = frame_result.unwrap();
            info!("Server received frame: {:?}", frame);

            match frame.frame_type {
                FrameType::Push => {
                    info!("Server received push message: {:?}", frame.payload);
                }
                FrameType::Req => {
                    info!("Server received request, sending response");
                    let response = Frame::response(frame.msgno, "response");
                    debug!("Server created response frame: {:?}", response);
                    debug!("Server sending response frame");
                    conn.sender().send(response).await.unwrap();
                    debug!("Server sent response successfully");
                }
                FrameType::Close => {
                    info!("Server received Close frame, closing connection");
                    conn.close().await.unwrap();
                    break;
                }
                _ => {
                    debug!("Server received unexpected frame type: {:?}", frame.frame_type);
                }
            }
        }
        info!("Server exiting");
    });

    // Connect client
    let client = Client::new_insecure("localhost", format!("127.0.0.1:{}", server_addr.port())).unwrap();
    let conn = client.connect().await.unwrap();
    info!("Client connected");

    // Send push message
    info!("Client sending push message");
    conn.push("push message").await.unwrap();
    debug!("Client push message sent");

    // Send request and wait for response
    info!("Client sending request");
    debug!("Client creating request");
    let response = conn.request("request").await.unwrap();
    info!("Client received response: {:?}", response);
    assert_eq!(&response.payload[..], b"response");

    // Send close frame and close connection
    info!("Client sending Close frame");
    conn.sender().send(Frame::close()).await.unwrap();
    info!("Client closing connection");
    conn.close().await.unwrap();

    // Wait for server to complete
    info!("Waiting for server to exit");
    server_handle.await.unwrap();
    info!("Test complete");
}
