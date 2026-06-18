//! Low-level HTTP bindings to the Segment tracking API.

use crate::Client;
use crate::Message;
use crate::Result;
use std::time::Duration;

/// A client which synchronously sends single messages to the Segment tracking
/// API.
///
/// `HttpClient` implements [`Client`](../client/trait.Client.html); see the
/// documentation for `Client` for more on how to send events to Segment.
#[derive(Clone, Debug)]
pub struct HttpClient {
    client: reqwest::Client,
    host: String,
}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::new(10, 0))
                .build()
                .unwrap(),
            host: "https://api.segment.io".to_owned(),
        }
    }
}

impl HttpClient {
    /// Construct a new `HttpClient` from a `reqwest::Client` and a Segment API
    /// scheme and host.
    ///
    /// If you don't care to re-use an existing `reqwest::Client`, you can use
    /// the `Default::default` value, which will send events to
    /// `https://api.segment.io`.
    pub fn new(client: reqwest::Client, host: String) -> HttpClient {
        HttpClient { client, host }
    }

    fn subpath_from_msg_type(msg: &Message) -> &'static str {
        match msg {
            Message::Identify(_) => "/v1/identify",
            Message::Track(_) => "/v1/track",
            Message::Page(_) => "/v1/page",
            Message::Screen(_) => "/v1/screen",
            Message::Group(_) => "/v1/group",
            Message::Alias(_) => "/v1/alias",
            Message::Batch(_) => "/v1/batch",
        }
    }

    fn path_from_msg_type(&self, msg: &Message) -> String {
        let sub_path: &'static str = Self::subpath_from_msg_type(msg);
        format!("{}{}", self.host, sub_path)
    }

    fn new_send_request(&self, write_key: &str, msg: &Message) -> reqwest::RequestBuilder {
        let path: String = self.path_from_msg_type(&msg);

        self.client
            .post(&path)
            .basic_auth(write_key, Some(""))
            .json(&msg)
    }

    fn backoff_delay(attempt: u8) -> Duration {
        match attempt {
            0 => Duration::from_millis(250),
            1 => Duration::from_millis(500),
            _ => Duration::from_millis(1000),
        }
    }
}

#[async_trait::async_trait]
impl Client for HttpClient {
    async fn send(&self, write_key: String, msg: Message) -> Result<()> {
        const ATTEMPTS: u8 = 3;

        let mut last_error: Option<reqwest::Error> = None;

        /*
           Do attempts because sometimes the network channel may break due an ungraceful transport
           closure:

            source: hyper_util::client::legacy::Error(
                SendRequest,
                hyper::Error(
                    IncompleteMessage,
                ),
            ),
        */

        for i in 0..ATTEMPTS {
            let request = self.new_send_request(&write_key, &msg);
            let send_result: std::result::Result<reqwest::Response, reqwest::Error> =
                request.send().await;

            match send_result {
                Ok(r) => {
                    let server_result = r.error_for_status();

                    match server_result {
                        Ok(_) => return Ok(()),
                        Err(e) => {
                            last_error = Some(e);

                            let retryable = status.is_server_error()
                                || status == reqwest::StatusCode::TOO_MANY_REQUESTS;
                            if !retryable {
                                break;
                            }

                            let delay = Self::backoff_delay(i);
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                    }
                }
                Err(e) => {
                    last_error = Some(e);
                    let delay = Self::backoff_delay(i);
                    tokio::time::sleep(delay).await;
                    continue;
                }
            }
        }

        match last_error {
            Some(e) => Err(e.into()),
            None => unreachable!(),
        }
    }
}
