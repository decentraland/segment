use std::{collections::VecDeque, path::Path};

use rusqlite::{params, Connection};
use thiserror::Error;

use crate::Message;

const DEFAULT_EVENT_COUNT_LIMIT: u32 = 200;

#[derive(Error, Debug)]
pub enum AnalyticsError {}

#[derive(Error, Debug)]
pub enum EnqueError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serialize json error {0}")]
    Json(#[from] serde_json::Error),
    #[error("limit reached error")]
    LimitReached,
}

#[derive(Error, Debug)]
pub enum PeekError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("deserialize json error {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct AnalyticsEvent {
    pub id: u64,
    pub message: Message,
}

pub trait AnalyticsEventQueue {
    fn enque(&mut self, msg: Message) -> Result<(), EnqueError>;

    fn peek(&self) -> Result<Option<AnalyticsEvent>, PeekError>;

    fn consume(&mut self, id: u64);
}

pub struct PersistentAnalyticsEventQueue {
    conn: Connection,
    event_count_limit: u32,
}

impl PersistentAnalyticsEventQueue {
    pub fn new<P: AsRef<Path>>(path: P, event_count_limit: u32) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?; // "Cannot open db"

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS analytics_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL DEFAULT (DATETIME('now')),
                message TEXT NOT NULL
            )",
        )?; // Cannot create table

        Ok(Self {
            conn,
            event_count_limit,
        })
    }
}

impl AnalyticsEventQueue for PersistentAnalyticsEventQueue {
    fn enque(&mut self, msg: Message) -> Result<(), EnqueError> {
        let json = serde_json::to_string(&msg)?;

        self.conn.execute(
            "INSERT INTO analytics_events (message) VALUES (?1)",
            params![json],
        )?;

        let count: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM analytics_events", [], |row| {
                    row.get(0)
                })?;

        if count > i64::from(self.event_count_limit) {
            let to_delete = count.saturating_sub(i64::from(self.event_count_limit));
            self.conn.execute(
                "DELETE FROM analytics_events WHERE id IN (SELECT id FROM analytics_events ORDER BY timestamp ASC LIMIT ?1)",
                params![to_delete],
            )?;
        }

        Ok(())
    }

    fn peek(&self) -> Result<Option<AnalyticsEvent>, PeekError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, message 
                 FROM analytics_events 
                 ORDER BY timestamp DESC 
                 LIMIT 1",
        )?;

        let mut rows = stmt.query([])?;
        match rows.next() {
            Ok(row) => {
                if let Some(row) = row {
                    let id: u64 = row.get(0)?;
                    let text: String = row.get(1)?;
                    let message: Message = serde_json::from_str(&text)?;
                    Ok(Some(AnalyticsEvent { id, message }))
                } else {
                    Ok(None)
                }
            }

            Err(e) => Err(e.into()),
        }
    }

    fn consume(&mut self, id: u64) {
        let _ = self
            .conn
            .execute("DELETE FROM analytics_events WHERE id = ?1", params![id]);
    }
}

pub struct InMemoryAnalyticsEventQueue {
    events: VecDeque<AnalyticsEvent>,
    next_id: u64,
    event_count_limit: usize,
}

impl InMemoryAnalyticsEventQueue {
    pub const fn new(event_count_limit: u32) -> Self {
        Self {
            events: VecDeque::new(),
            next_id: 1,
            event_count_limit: event_count_limit as usize,
        }
    }

    fn trim_oldest(&mut self) {
        while self.events.len() > self.event_count_limit {
            self.events.pop_front();
        }
    }
}

impl AnalyticsEventQueue for InMemoryAnalyticsEventQueue {
    fn enque(&mut self, msg: Message) -> Result<(), EnqueError> {
        let event = AnalyticsEvent {
            id: self.next_id,
            message: msg,
        };

        match self.next_id.checked_add(1) {
            Some(new_value) => {
                self.next_id = new_value;
            }
            None => {
                return Err(EnqueError::LimitReached);
            }
        }

        self.events.push_back(event);
        self.trim_oldest();

        Ok(())
    }

    fn peek(&self) -> Result<Option<AnalyticsEvent>, PeekError> {
        Ok(self.events.back().cloned())
    }

    fn consume(&mut self, id: u64) {
        if let Some(pos) = self.events.iter().position(|e| e.id == id) {
            self.events.remove(pos);
        }
    }
}

pub enum CombinedAnalyticsEventQueue {
    Persistent(PersistentAnalyticsEventQueue),
    InMemory(InMemoryAnalyticsEventQueue),
}

impl AnalyticsEventQueue for CombinedAnalyticsEventQueue {
    fn enque(&mut self, msg: Message) -> Result<(), EnqueError> {
        match self {
            Self::Persistent(queue) => queue.enque(msg),
            Self::InMemory(queue) => queue.enque(msg),
        }
    }

    fn peek(&self) -> Result<Option<AnalyticsEvent>, PeekError> {
        match self {
            Self::Persistent(queue) => queue.peek(),
            Self::InMemory(queue) => queue.peek(),
        }
    }

    fn consume(&mut self, id: u64) {
        match self {
            Self::Persistent(queue) => {
                queue.consume(id);
            }
            Self::InMemory(queue) => {
                queue.consume(id);
            }
        }
    }
}

pub enum CombinedAnalyticsEventQueueNewResult {
    Persistent(CombinedAnalyticsEventQueue),
    FallbackToInMemory(CombinedAnalyticsEventQueue, rusqlite::Error),
}

impl CombinedAnalyticsEventQueue {
    pub fn new<P: AsRef<Path>>(
        path: P,
        event_count_limit: Option<u32>,
    ) -> CombinedAnalyticsEventQueueNewResult {
        let event_count_limit = event_count_limit.unwrap_or(DEFAULT_EVENT_COUNT_LIMIT);
        let persistent = PersistentAnalyticsEventQueue::new(path, event_count_limit);

        match persistent {
            Ok(persistent) => {
                CombinedAnalyticsEventQueueNewResult::Persistent(Self::Persistent(persistent))
            }
            Err(e) => CombinedAnalyticsEventQueueNewResult::FallbackToInMemory(
                Self::InMemory(InMemoryAnalyticsEventQueue::new(event_count_limit)),
                e,
            ),
        }
    }
}
