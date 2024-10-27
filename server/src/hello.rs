use axum::{http::StatusCode, response::IntoResponse};

pub async fn hello() -> impl IntoResponse {
    let html = r#"
        <!DOCTYPE html>
        <html>
        <head>
            <title>Ankurah Server</title>
            <style>
                body {
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
                    max-width: 800px;
                    margin: 40px auto;
                    padding: 0 20px;
                    line-height: 1.6;
                    color: #333;
                }
                .container {
                    background: #f5f5f5;
                    border-radius: 8px;
                    padding: 20px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }
                h1 {
                    color: #2c3e50;
                    margin-bottom: 16px;
                }
                .info {
                    color: #666;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Welcome to Ankurah Server</h1>
                <p class="info">
                    This is the Ankurah server. Your application should connect 
                    to it using the web-client crate.
                </p>
                <p class="info">
                    For more information, please refer to <code>GETTING_STARTED.md</code>.
                </p>
            </div>
        </body>
        </html>
    "#;

    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/html")],
        html,
    )
}
