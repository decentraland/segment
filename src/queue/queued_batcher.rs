use std::sync::Arc;
use tokio::sync::Mutex;

use serde_json::Value;

use crate::{
    message::BatchMessage,
    queue::event_queue::{AnalyticsEventQueue, EnqueError},
    Batcher, Result,
};

pub struct QueuedBatcher {
    queue: Arc<Mutex<dyn AnalyticsEventQueue + Send>>,
    batcher: Batcher,
    context: Option<Value>,
}

impl QueuedBatcher {
    pub fn new(queue: Arc<Mutex<dyn AnalyticsEventQueue + Send>>, context: Option<Value>) -> Self {
        Self {
            queue,
            batcher: Batcher::new(context.clone()),
            context,
        }
    }

    // Enqueues the event, doesn't send instantly
    pub fn push(&mut self, msg: impl Into<BatchMessage>) -> Result<Option<BatchMessage>> {
        self.batcher.push(msg)
    }

    pub async fn flush(&mut self) -> std::result::Result<(), EnqueError> {
        if self.batcher.is_empty() {
            return Ok(());
        }

        let batcher = std::mem::replace(&mut self.batcher, Batcher::new(self.context.clone()));
        let message = batcher.into_message();
        self.queue.lock().await.enque(message)
    }
}
