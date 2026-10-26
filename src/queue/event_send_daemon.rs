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
const EMPTY_QUEUE_POLL_DELAY: Duration = Duration::from_millis(200);

#[derive(Error, Debug)]
pub enum SendError {
    /// The row can never be sent and is removed.
    #[error("message {item_id} can never be sent: {source}")]
    Dropped { item_id: u64, source: crate::Error },
    /// The queue removed a row it could not read.
    #[error("unreadable row removed: {0}")]
    UnreadableRow(serde_json::Error),
    /// The API is unreachable or asks to retry later; the row stays.
    #[error("network error for message {item_id}: {source}")]
    Network {
        item_id: u64,
        source: reqwest::Error,
    },
    /// The API refused the request for a reason that is not the message, such as an invalid write key; the row stays.
    #[error("message {item_id} rejected: {source}")]
    Rejected {
        item_id: u64,
        source: reqwest::Error,
    },
    /// The queue could not be read; the row stays.
    #[error("sqlite error: {0}")]
    Queue(rusqlite::Error),
    /// The row was sent or dropped but could not be removed; it stays and is not sent again.
    #[error("message {item_id} cannot be removed: {source}")]
    Unremoved {
        item_id: u64,
        source: rusqlite::Error,
    },
}

enum SendResult {
    Sent,
    QueueEmpty,
}

/// A delay that doubles on every failure up to a cap and returns to its start on success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RetryBackOff {
    initial: Duration,
    current: Duration,
}

impl RetryBackOff {
    const MAX: Duration = Duration::from_secs(60);

    const fn new(initial: Duration) -> Self {
        Self {
            initial,
            current: initial,
        }
    }

    const fn current(&self) -> Duration {
        self.current
    }

    fn grow(&mut self) {
        self.current = self.current.saturating_mul(2).min(Self::MAX);
    }

    fn reset(&mut self) {
        self.current = self.initial;
    }
}

pub struct AnalyticsEventSendDaemon<TClient: Client + Send> {
    queue: Arc<Mutex<dyn AnalyticsEventQueue + Send>>,
    process_delay: Duration,
    write_key: String,
    client: Arc<Mutex<TClient>>,
    task: Option<JoinHandle<()>>,
}

impl<TClient: Client + Send + 'static> AnalyticsEventSendDaemon<TClient> {
    pub fn start<EF>(&mut self, async_runtime: tokio::runtime::Handle, error_log_fn: EF)
    where
        EF: Fn(&str) + Send + Sync + 'static,
    {
        self.stop();

        let client = self.client.clone();
        let queue = self.queue.clone();
        let write_key = self.write_key.clone();
        let mut back_off = RetryBackOff::new(self.process_delay);
        // A row that was sent or dropped but could not be removed: it is only removed again, never sent again.
        let mut settled: Option<u64> = None;
        // A condition that persists across retries is reported once per failure streak.
        let mut streak_reported = false;

        let handle = async_runtime.spawn(async move {
            loop {
                match Self::send(queue.clone(), client.clone(), write_key.clone(), settled).await {
                    Ok(SendResult::Sent) => {
                        settled = None;
                        streak_reported = false;
                        back_off.reset();
                    }
                    Ok(SendResult::QueueEmpty) => {
                        settled = None;
                        streak_reported = false;
                        back_off.reset();
                        sleep(EMPTY_QUEUE_POLL_DELAY).await;
                    }
                    Err(e) => match &e {
                        SendError::Dropped { item_id, .. } => {
                            error_log_fn(
                                format!("Error executing send loop (will drop): {e:#?}").as_str(),
                            );

                            // A row that stays would be peeked again at once; wait as after a failed read.
                            if let Err(e) = queue.lock().await.consume(*item_id) {
                                settled = Some(*item_id);
                                report_first_of_streak(
                                    &mut streak_reported,
                                    &error_log_fn,
                                    format!("Error executing send loop (cannot drop, will retry): {e:#?}")
                                        .as_str(),
                                );
                                sleep(back_off.current()).await;
                                back_off.grow();
                            }
                        }
                        SendError::UnreadableRow(_) => {
                            error_log_fn(
                                format!(
                                    "Error executing send loop (dropped an unreadable row): {e:#?}"
                                )
                                .as_str(),
                            );
                        }
                        SendError::Network { .. } => {
                            sleep(back_off.current()).await;
                            back_off.grow();
                        }
                        SendError::Rejected { .. } => {
                            report_first_of_streak(
                                &mut streak_reported,
                                &error_log_fn,
                                format!("Error executing send loop (rejected, will retry): {e:#?}")
                                    .as_str(),
                            );
                            sleep(back_off.current()).await;
                            back_off.grow();
                        }
                        SendError::Queue(_) | SendError::Unremoved { .. } => {
                            if let SendError::Unremoved { item_id, .. } = &e {
                                settled = Some(*item_id);
                            }
                            report_first_of_streak(
                                &mut streak_reported,
                                &error_log_fn,
                                format!("Error executing send loop (will retry): {e:#?}").as_str(),
                            );
                            sleep(back_off.current()).await;
                            back_off.grow();
                        }
                    },
                }
            }
        });

        self.task = Some(handle);
    }
}

fn report_first_of_streak<EF: Fn(&str)>(
    streak_reported: &mut bool,
    error_log_fn: &EF,
    message: &str,
) {
    if !*streak_reported {
        *streak_reported = true;
        error_log_fn(message);
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
        settled: Option<u64>,
    ) -> std::result::Result<SendResult, SendError> {
        let AnalyticsEvent { id, message } = match queue.lock().await.peek() {
            Ok(Some(event)) => event,
            Ok(None) => return Ok(SendResult::QueueEmpty),
            Err(PeekError::Json(e)) => return Err(SendError::UnreadableRow(e)),
            Err(PeekError::Sqlite(e)) => return Err(SendError::Queue(e)),
        };

        if settled != Some(id) {
            client
                .lock()
                .await
                .send(write_key, message)
                .await
                .map_err(|source| classify_send_failure(id, source))?;
        }

        queue
            .lock()
            .await
            .consume(id)
            .map_err(|source| SendError::Unremoved {
                item_id: id,
                source,
            })?;
        Ok(SendResult::Sent)
    }
}

/// A 400 answer or a message the API can never take is dropped. An answer with no status, a 5xx, 408 or 429 is a
/// connectivity failure; any other status is a rejection of the request itself.
fn classify_send_failure(item_id: u64, error: crate::Error) -> SendError {
    let crate::Error::NetworkError(source) = error else {
        return SendError::Dropped {
            item_id,
            source: error,
        };
    };

    match source.status() {
        Some(reqwest::StatusCode::BAD_REQUEST) => SendError::Dropped {
            item_id,
            source: crate::Error::NetworkError(source),
        },
        Some(status)
            if !status.is_server_error()
                && status != reqwest::StatusCode::REQUEST_TIMEOUT
                && status != reqwest::StatusCode::TOO_MANY_REQUESTS =>
        {
            SendError::Rejected { item_id, source }
        }
        _ => SendError::Network { item_id, source },
    }
}

impl<TClient: Client + Send> Drop for AnalyticsEventSendDaemon<TClient> {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Track;
    use crate::queue::event_queue::{EnqueError, InMemoryAnalyticsEventQueue};
    use crate::Message;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct UnreadableQueue;

    impl AnalyticsEventQueue for UnreadableQueue {
        fn enque(&mut self, _msg: Message) -> Result<(), EnqueError> {
            Ok(())
        }

        fn peek(&mut self) -> Result<Option<AnalyticsEvent>, PeekError> {
            Err(PeekError::Sqlite(rusqlite::Error::QueryReturnedNoRows))
        }

        fn consume(&mut self, _id: u64) -> Result<(), rusqlite::Error> {
            Ok(())
        }
    }

    struct EmptyQueue;

    impl AnalyticsEventQueue for EmptyQueue {
        fn enque(&mut self, _msg: Message) -> Result<(), EnqueError> {
            Ok(())
        }

        fn peek(&mut self) -> Result<Option<AnalyticsEvent>, PeekError> {
            Ok(None)
        }

        fn consume(&mut self, _id: u64) -> Result<(), rusqlite::Error> {
            Ok(())
        }
    }

    struct CountingEmptyQueue(Arc<AtomicUsize>);

    impl AnalyticsEventQueue for CountingEmptyQueue {
        fn enque(&mut self, _msg: Message) -> Result<(), EnqueError> {
            Ok(())
        }

        fn peek(&mut self) -> Result<Option<AnalyticsEvent>, PeekError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }

        fn consume(&mut self, _id: u64) -> Result<(), rusqlite::Error> {
            Ok(())
        }
    }

    /// Always holds one row that cannot be removed, as a queue on a full or read-only disk does. Counts the removals.
    struct StuckRowQueue(Arc<AtomicUsize>);

    impl AnalyticsEventQueue for StuckRowQueue {
        fn enque(&mut self, _msg: Message) -> Result<(), EnqueError> {
            Ok(())
        }

        fn peek(&mut self) -> Result<Option<AnalyticsEvent>, PeekError> {
            Ok(Some(AnalyticsEvent {
                id: 1,
                message: track(),
            }))
        }

        fn consume(&mut self, _id: u64) -> Result<(), rusqlite::Error> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
                None,
            ))
        }
    }

    struct NoopClient;

    #[async_trait::async_trait]
    impl Client for NoopClient {
        async fn send(&self, _write_key: String, _msg: Message) -> crate::Result<()> {
            Ok(())
        }
    }

    /// Fails every send with `failure` until `failures_left` is spent, counting the attempts.
    struct FlakyClient {
        failure: fn() -> crate::Error,
        failures_left: AtomicUsize,
        attempts: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Client for FlakyClient {
        async fn send(&self, _write_key: String, _msg: Message) -> crate::Result<()> {
            self.attempts.fetch_add(1, Ordering::Relaxed);

            if self.failures_left.load(Ordering::Relaxed) == 0 {
                return Ok(());
            }

            self.failures_left.fetch_sub(1, Ordering::Relaxed);
            Err((self.failure)())
        }
    }

    /// A client that fails to build yields a request error with no status, which is what an unreachable API answers with.
    fn network_error() -> crate::Error {
        let error = reqwest::Client::builder()
            .user_agent("\u{0}")
            .build()
            .expect_err("an invalid user agent fails the client build");
        crate::Error::NetworkError(error)
    }

    fn status_error(status: u16) -> reqwest::Error {
        let response = http::Response::builder()
            .status(status)
            .body("")
            .expect("a status and an empty body make a valid response");
        reqwest::Response::from(response)
            .error_for_status()
            .expect_err("the status is an error status")
    }

    fn unauthorized() -> crate::Error {
        crate::Error::NetworkError(status_error(401))
    }

    fn daemon(
        queue: impl AnalyticsEventQueue + Send + 'static,
    ) -> AnalyticsEventSendDaemon<NoopClient> {
        let queue: Arc<Mutex<dyn AnalyticsEventQueue + Send>> = Arc::new(Mutex::new(queue));
        AnalyticsEventSendDaemon::new(queue, None, "write-key".to_owned(), NoopClient)
    }

    fn track() -> Message {
        Message::Track(Track {
            timestamp: Some(time::OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        })
    }

    /// A queue that cannot be read has to leave through the timeout. A
    /// regression shows up as a test that never finishes, since the loop it
    /// guards against has no await point for the harness to cancel.
    #[tokio::test]
    async fn unreadable_queue_leaves_through_the_timeout() {
        let timeout = Duration::from_millis(200);
        let daemon = daemon(UnreadableQueue);

        let start = Instant::now();
        daemon
            .wait_until_empty_queue_or_abandon(Some(timeout))
            .await;
        let elapsed = start.elapsed();

        assert!(
            elapsed >= timeout,
            "gave up before the timeout: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn empty_queue_returns_without_waiting_for_the_timeout() {
        let timeout = Duration::from_secs(30);
        let daemon = daemon(EmptyQueue);

        let start = Instant::now();
        daemon
            .wait_until_empty_queue_or_abandon(Some(timeout))
            .await;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(1),
            "waited too long: {elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sender_waits_between_empty_queue_reads() {
        let reads = Arc::new(AtomicUsize::new(0));
        let mut daemon = daemon(CountingEmptyQueue(reads.clone()));
        daemon.start(tokio::runtime::Handle::current(), |_| {
            panic!("Empty queue is not an error")
        });

        for _ in 0..10 {
            tokio::task::yield_now().await;
            if reads.load(Ordering::Relaxed) > 0 {
                break;
            }
        }
        assert_eq!(reads.load(Ordering::Relaxed), 1);

        tokio::time::advance(EMPTY_QUEUE_POLL_DELAY - Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(reads.load(Ordering::Relaxed), 1);

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(reads.load(Ordering::Relaxed), 2);

        daemon.stop();
    }

    #[test]
    fn back_off_doubles_up_to_the_cap_and_resets() {
        let mut back_off = RetryBackOff::new(DEFAULT_PROCESS_DELAY_AFTER_ERROR);
        let mut seen = Vec::new();

        for _ in 0..12 {
            seen.push(back_off.current());
            back_off.grow();
        }
        back_off.reset();

        assert_eq!(seen[0], Duration::from_millis(200));
        assert_eq!(seen[1], Duration::from_millis(400));
        assert_eq!(seen[2], Duration::from_millis(800));
        assert_eq!(seen[9], RetryBackOff::MAX);
        assert_eq!(seen[11], RetryBackOff::MAX);
        assert_eq!(back_off.current(), DEFAULT_PROCESS_DELAY_AFTER_ERROR);
    }

    #[tokio::test(start_paused = true)]
    async fn a_network_failure_is_retried_with_back_off_and_is_not_a_fault() {
        let mut queue = InMemoryAnalyticsEventQueue::new(10);
        queue.enque(track()).unwrap();
        let queue: Arc<Mutex<dyn AnalyticsEventQueue + Send>> = Arc::new(Mutex::new(queue));
        let attempts = Arc::new(AtomicUsize::new(0));
        let client = FlakyClient {
            failure: network_error,
            failures_left: AtomicUsize::new(3),
            attempts: attempts.clone(),
        };
        let mut daemon =
            AnalyticsEventSendDaemon::new(queue.clone(), None, "write-key".to_owned(), client);
        daemon.start(tokio::runtime::Handle::current(), |error| {
            panic!("a network failure is not a fault: {error}")
        });

        // Three failures cost 200 + 400 + 800 ms of back-off; the fourth attempt succeeds.
        for _ in 0..10 {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        let attempts_after_one_second = attempts.load(Ordering::Relaxed);
        for _ in 0..30 {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(100)).await;
        }

        assert_eq!(attempts_after_one_second, 3);
        assert_eq!(attempts.load(Ordering::Relaxed), 4);
        assert!(queue.lock().await.peek().unwrap().is_none());
        daemon.stop();
    }

    /// A sent row that stays in the queue is peeked again; the loop must wait between the removals instead of spinning,
    /// never send the row again and report the streak once.
    #[tokio::test(start_paused = true)]
    async fn a_row_that_cannot_be_removed_is_not_sent_again() {
        let removals = Arc::new(AtomicUsize::new(0));
        let sends = Arc::new(AtomicUsize::new(0));
        let faults = Arc::new(AtomicUsize::new(0));
        let sink = faults.clone();
        let queue: Arc<Mutex<dyn AnalyticsEventQueue + Send>> =
            Arc::new(Mutex::new(StuckRowQueue(removals.clone())));
        let client = FlakyClient {
            failure: network_error,
            failures_left: AtomicUsize::new(0),
            attempts: sends.clone(),
        };
        let mut daemon = AnalyticsEventSendDaemon::new(queue, None, "write-key".to_owned(), client);
        daemon.start(tokio::runtime::Handle::current(), move |error| {
            assert!(error.contains("will retry"), "{error}");
            sink.fetch_add(1, Ordering::Relaxed);
        });

        // Three failures cost 200 + 400 + 800 ms of back-off, so one second holds three removals.
        for _ in 0..10 {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(100)).await;
        }

        assert_eq!(removals.load(Ordering::Relaxed), 3);
        assert_eq!(sends.load(Ordering::Relaxed), 1);
        assert_eq!(faults.load(Ordering::Relaxed), 1);
        daemon.stop();
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejected_request_is_retried_and_reported_once_per_streak() {
        let mut queue = InMemoryAnalyticsEventQueue::new(10);
        queue.enque(track()).unwrap();
        let queue: Arc<Mutex<dyn AnalyticsEventQueue + Send>> = Arc::new(Mutex::new(queue));
        let attempts = Arc::new(AtomicUsize::new(0));
        let faults = Arc::new(AtomicUsize::new(0));
        let sink = faults.clone();
        let client = FlakyClient {
            failure: unauthorized,
            failures_left: AtomicUsize::new(3),
            attempts: attempts.clone(),
        };
        let mut daemon =
            AnalyticsEventSendDaemon::new(queue.clone(), None, "write-key".to_owned(), client);
        daemon.start(tokio::runtime::Handle::current(), move |error| {
            assert!(error.contains("rejected, will retry"), "{error}");
            sink.fetch_add(1, Ordering::Relaxed);
        });

        // Three failures cost 200 + 400 + 800 ms of back-off; the fourth attempt succeeds.
        for _ in 0..40 {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(100)).await;
        }

        assert_eq!(attempts.load(Ordering::Relaxed), 4);
        assert_eq!(faults.load(Ordering::Relaxed), 1);
        assert!(queue.lock().await.peek().unwrap().is_none());
        daemon.stop();
    }

    #[test]
    fn send_failures_are_classified_by_status() {
        for status in [401, 403, 404, 413, 422] {
            let failure =
                classify_send_failure(1, crate::Error::NetworkError(status_error(status)));
            assert!(
                matches!(failure, SendError::Rejected { .. }),
                "{status}: {failure:?}"
            );
        }

        for status in [408, 429, 500, 503] {
            let failure =
                classify_send_failure(1, crate::Error::NetworkError(status_error(status)));
            assert!(
                matches!(failure, SendError::Network { .. }),
                "{status}: {failure:?}"
            );
        }

        let failure = classify_send_failure(1, crate::Error::NetworkError(status_error(400)));
        assert!(matches!(failure, SendError::Dropped { .. }), "{failure:?}");

        let failure = classify_send_failure(1, network_error());
        assert!(matches!(failure, SendError::Network { .. }), "{failure:?}");

        let failure = classify_send_failure(1, crate::Error::MessageTooLarge);
        assert!(matches!(failure, SendError::Dropped { .. }), "{failure:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_message_the_api_can_never_take_is_dropped_and_reported() {
        struct RejectingClient;

        #[async_trait::async_trait]
        impl Client for RejectingClient {
            async fn send(&self, _write_key: String, _msg: Message) -> crate::Result<()> {
                Err(crate::Error::MessageTooLarge)
            }
        }

        let mut queue = InMemoryAnalyticsEventQueue::new(10);
        queue.enque(track()).unwrap();
        let queue: Arc<Mutex<dyn AnalyticsEventQueue + Send>> = Arc::new(Mutex::new(queue));
        let faults = Arc::new(AtomicUsize::new(0));
        let sink = faults.clone();
        let mut daemon = AnalyticsEventSendDaemon::new(
            queue.clone(),
            None,
            "write-key".to_owned(),
            RejectingClient,
        );
        daemon.start(tokio::runtime::Handle::current(), move |error| {
            assert!(error.contains("will drop"), "{error}");
            sink.fetch_add(1, Ordering::Relaxed);
        });

        for _ in 0..10 {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(100)).await;
        }

        assert_eq!(faults.load(Ordering::Relaxed), 1);
        assert!(queue.lock().await.peek().unwrap().is_none());
        daemon.stop();
    }
}
