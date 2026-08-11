use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use thiserror::Error;
use tokio::{sync::Mutex, task::JoinHandle, time::sleep};

use crate::{
    queue::event_queue::{AnalyticsEvent, AnalyticsEventQueue, PeekError},
    Client,
};

const DEFAULT_PROCESS_DELAY_AFTER_ERROR: Duration = Duration::from_millis(200);

#[derive(Error, Debug)]
pub enum SendError {
    #[error("sqlite error: {0}")]
    QueueError(PeekError),
    #[error("network client error: {segment_error} {item_id}")]
    ClientError {
        segment_error: crate::Error,
        item_id: u64,
    },
}

pub struct AnalyticsEventSendDaemon<TClient: Client + Send> {
    queue: Arc<Mutex<dyn AnalyticsEventQueue + Send>>,
    process_delay: Duration,
    write_key: String,
    client: Arc<Mutex<TClient>>,
    task: Option<JoinHandle<()>>,
}

impl<TClient: Client + Send + 'static> AnalyticsEventSendDaemon<TClient> {
    pub fn start<EF>(&mut self, error_log_fn: EF)
    where
        EF: Fn(&str) + Send + Sync + 'static,
    {
        self.stop();

        let client = self.client.clone();
        let queue = self.queue.clone();
        let write_key = self.write_key.clone();
        let process_delay = self.process_delay;

        let handle = tokio::spawn(async move {
            loop {
                let result = Self::send(queue.clone(), client.clone(), write_key.clone()).await;
                if let Err(e) = result {
                    let drop_item_id = should_drop(&e);

                    if let Some(drop_item_id) = drop_item_id {
                        error_log_fn(
                            format!("Error executing send loop (will drop): {:#?}", e).as_str(),
                        );
                        queue.lock().await.consume(drop_item_id);
                    } else {
                        error_log_fn(
                            format!("Error executing send loop (will retry): {:#?}", e).as_str(),
                        );
                    }

                    sleep(process_delay).await;
                }
            }
        });

        self.task = Some(handle);
    }
}

impl<TClient: Client + Send> AnalyticsEventSendDaemon<TClient> {
    pub fn new(
        queue: Arc<Mutex<dyn AnalyticsEventQueue + Send>>,
        process_delay: Option<Duration>,
        write_key: String,
        client: TClient,
    ) -> Self {
        Self {
            queue,
            process_delay: process_delay.unwrap_or(DEFAULT_PROCESS_DELAY_AFTER_ERROR),
            write_key,
            client: Arc::new(Mutex::new(client)),
            task: None,
        }
    }

    pub fn stop(&mut self) {
        if let Some(task) = &self.task {
            // TODO use notify for graceful cancellation
            task.abort();
            self.task = None;
        }
    }

    pub async fn wait_until_empty_queue_or_abandon(&self, timeout: Option<Duration>) {
        const CHECK_PERIOD: Duration = Duration::from_millis(50);
        let timeout = timeout.unwrap_or(Duration::from_millis(500));

        // if cannot add timeout expiry happens immediately
        let expiry = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);

        loop {
            // A failed read means "not empty yet" and must still reach the
            // expiry check and the sleep below, so a queue locked by another
            // process cannot spin this loop.
            let drained = matches!(self.queue.lock().await.peek(), Ok(None));

            if drained || Instant::now() >= expiry {
                break;
            }

            sleep(CHECK_PERIOD).await;
        }
    }

    async fn send(
        queue: Arc<Mutex<dyn AnalyticsEventQueue + Send>>,
        client: Arc<Mutex<TClient>>,
        write_key: String,
    ) -> std::result::Result<(), SendError> {
        let event = queue.lock().await.peek();

        match event {
            Ok(event) => {
                if let Some(event) = event {
                    let AnalyticsEvent { id, message } = event;
                    if let Err(e) = client.lock().await.send(write_key, message).await {
                        Err(SendError::ClientError {
                            segment_error: e,
                            item_id: id,
                        })
                    } else {
                        queue.lock().await.consume(id);
                        Ok(())
                    }
                } else {
                    Ok(())
                }
            }
            Err(error) => Err(SendError::QueueError(error)),
        }
    }
}

impl<TClient: Client + Send> Drop for AnalyticsEventSendDaemon<TClient> {
    fn drop(&mut self) {
        self.stop();
    }
}

// Drop if http response 400
fn should_drop(error: &SendError) -> Option<u64> {
    if let SendError::ClientError {
        segment_error,
        item_id,
    } = error
    {
        if let crate::Error::NetworkError(network_error) = segment_error {
            let status = network_error.status();
            if let Some(status_code) = status {
                if status_code == reqwest::StatusCode::BAD_REQUEST {
                    return Some(*item_id);
                }
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::event_queue::EnqueError;
    use crate::Message;

    struct UnreadableQueue;

    impl AnalyticsEventQueue for UnreadableQueue {
        fn enque(&mut self, _msg: Message) -> Result<(), EnqueError> {
            Ok(())
        }

        fn peek(&self) -> Result<Option<AnalyticsEvent>, PeekError> {
            Err(PeekError::Json(
                serde_json::from_str::<u8>("not a number").unwrap_err(),
            ))
        }

        fn consume(&mut self, _id: u64) {}
    }

    struct EmptyQueue;

    impl AnalyticsEventQueue for EmptyQueue {
        fn enque(&mut self, _msg: Message) -> Result<(), EnqueError> {
            Ok(())
        }

        fn peek(&self) -> Result<Option<AnalyticsEvent>, PeekError> {
            Ok(None)
        }

        fn consume(&mut self, _id: u64) {}
    }

    struct NoopClient;

    #[async_trait::async_trait]
    impl Client for NoopClient {
        async fn send(&self, _write_key: String, _msg: Message) -> crate::Result<()> {
            Ok(())
        }
    }

    fn daemon(
        queue: impl AnalyticsEventQueue + Send + 'static,
    ) -> AnalyticsEventSendDaemon<NoopClient> {
        let queue: Arc<Mutex<dyn AnalyticsEventQueue + Send>> = Arc::new(Mutex::new(queue));
        AnalyticsEventSendDaemon::new(queue, None, "write-key".to_owned(), NoopClient)
    }

    /// A queue that cannot be read has to leave through the timeout. A
    /// regression shows up as a test that never finishes, since the loop it
    /// guards against has no await point for the harness to cancel.
    #[tokio::test]
    async fn unreadable_queue_leaves_through_the_timeout() {
        let timeout = Duration::from_millis(200);
        let daemon = daemon(UnreadableQueue);

        let start = Instant::now();
        daemon.wait_until_empty_queue_or_abandon(Some(timeout)).await;
        let elapsed = start.elapsed();

        assert!(elapsed >= timeout, "gave up before the timeout: {elapsed:?}");
    }

    #[tokio::test]
    async fn empty_queue_returns_without_waiting_for_the_timeout() {
        let timeout = Duration::from_secs(30);
        let daemon = daemon(EmptyQueue);

        let start = Instant::now();
        daemon.wait_until_empty_queue_or_abandon(Some(timeout)).await;
        let elapsed = start.elapsed();

        assert!(elapsed < Duration::from_secs(1), "waited too long: {elapsed:?}");
    }
}
