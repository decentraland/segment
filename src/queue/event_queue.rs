use std::{collections::VecDeque, path::Path, time::Duration};

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

    fn peek(&mut self) -> Result<Option<AnalyticsEvent>, PeekError>;

    /// Removes the event so the next peek moves past it. `Err` means the event is still there.
    fn consume(&mut self, id: u64) -> Result<(), rusqlite::Error>;
}

pub struct PersistentAnalyticsEventQueue {
    conn: Connection,
    event_count_limit: u32,
}

impl PersistentAnalyticsEventQueue {
    /// Opens the file as its only writer: the lock taken by the first write is kept until the connection closes,
    /// and a file locked by another connection fails at once with `SQLITE_BUSY` instead of waiting out a timeout.
    pub fn new<P: AsRef<Path>>(path: P, event_count_limit: u32) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::ZERO)?;
        conn.pragma_update(None, "locking_mode", "EXCLUSIVE")?;

        conn.execute_batch(
            "BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS analytics_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL DEFAULT (DATETIME('now')),
                message TEXT NOT NULL
            );
            COMMIT;",
        )?;

        Ok(Self {
            conn,
            event_count_limit,
        })
    }

    fn newest_row(&self) -> Result<Option<(u64, String)>, rusqlite::Error> {
        let mut stmt = self.conn.prepare(
            "SELECT id, message
                 FROM analytics_events
                 ORDER BY timestamp DESC
                 LIMIT 1",
        )?;

        let mut rows = stmt.query([])?;

        match rows.next()? {
            Some(row) => Ok(Some((row.get(0)?, row.get(1)?))),
            None => Ok(None),
        }
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

    /// A row that does not deserialize is removed, so the next peek moves past it. A row that cannot be removed is
    /// a sqlite failure, since the next peek would return it again.
    fn peek(&mut self) -> Result<Option<AnalyticsEvent>, PeekError> {
        let Some((id, text)) = self.newest_row()? else {
            return Ok(None);
        };

        match serde_json::from_str(&text) {
            Ok(message) => Ok(Some(AnalyticsEvent { id, message })),
            Err(e) => {
                self.consume(id)?;
                Err(PeekError::Json(e))
            }
        }
    }

    fn consume(&mut self, id: u64) -> Result<(), rusqlite::Error> {
        self.conn
            .execute("DELETE FROM analytics_events WHERE id = ?1", params![id])
            .map(|_| ())
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

    fn peek(&mut self) -> Result<Option<AnalyticsEvent>, PeekError> {
        Ok(self.events.back().cloned())
    }

    fn consume(&mut self, id: u64) -> Result<(), rusqlite::Error> {
        if let Some(pos) = self.events.iter().position(|e| e.id == id) {
            self.events.remove(pos);
        }

        Ok(())
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

    fn peek(&mut self) -> Result<Option<AnalyticsEvent>, PeekError> {
        match self {
            Self::Persistent(queue) => queue.peek(),
            Self::InMemory(queue) => queue.peek(),
        }
    }

    fn consume(&mut self, id: u64) -> Result<(), rusqlite::Error> {
        match self {
            Self::Persistent(queue) => queue.consume(id),
            Self::InMemory(queue) => queue.consume(id),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Track, User};
    use rusqlite::ErrorCode;

    fn track() -> Message {
        Message::Track(Track {
            user: User::AnonymousId {
                anonymous_id: "anon".to_owned(),
            },
            event: "test".to_owned(),
            timestamp: Some(time::OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        })
    }

    #[test]
    fn a_second_connection_to_the_same_file_is_refused_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite3");
        let _owner = PersistentAnalyticsEventQueue::new(&path, 10).unwrap();

        let started = std::time::Instant::now();
        let error = PersistentAnalyticsEventQueue::new(&path, 10).err().unwrap();

        assert_eq!(error.sqlite_error_code(), Some(ErrorCode::DatabaseBusy));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "waited for a busy timeout: {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn the_file_is_free_again_once_the_owner_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite3");
        let mut owner = PersistentAnalyticsEventQueue::new(&path, 10).unwrap();
        owner.enque(track()).unwrap();
        drop(owner);

        let mut next = PersistentAnalyticsEventQueue::new(&path, 10).unwrap();

        assert!(next.peek().unwrap().is_some());
    }

    #[test]
    fn a_file_that_is_not_a_database_is_reported_as_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite3");
        std::fs::write(
            &path,
            b"this is not a sqlite database at all, just some garbage bytes",
        )
        .unwrap();

        let error = PersistentAnalyticsEventQueue::new(&path, 10).err().unwrap();

        assert_eq!(error.sqlite_error_code(), Some(ErrorCode::NotADatabase));
    }

    #[test]
    fn an_unreadable_row_is_dropped_on_peek() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite3");
        drop(PersistentAnalyticsEventQueue::new(&path, 10).unwrap());
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO analytics_events (message) VALUES (?1)",
            params!["{ not json"],
        )
        .unwrap();
        drop(conn);
        let mut queue = PersistentAnalyticsEventQueue::new(&path, 10).unwrap();

        let first = queue.peek();
        let second = queue.peek();

        assert!(matches!(first, Err(PeekError::Json(_))));
        assert!(matches!(second, Ok(None)));
    }
}
