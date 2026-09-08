//! Atomic authorization identity and exact Issue occurrence location lookup.

use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};

use super::search::{self as authorization, Authorization};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OccurrenceLocation {
    pub issue_id: String,
    pub project_id: i64,
    pub shard_id: String,
    pub record_id: String,
    pub ingest_seq: i64,
}

#[derive(Debug, Clone)]
pub struct AuthorizedOccurrence {
    pub authorization: Authorization,
    pub location: OccurrenceLocation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IssueDetailError {
    NotFound,
    Corrupt,
}

impl std::fmt::Display for IssueDetailError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for IssueDetailError {}

/// Resolves an occurrence only when its Issue and project are active and internally consistent.
///
/// This runs inside one DbWorker closure, so no metadata mutation can interleave between the
/// authorization identity and occurrence location reads.
pub fn lookup(
    db: &Connection,
    actor: i64,
    issue_id: &str,
    record_id: &str,
) -> Result<AuthorizedOccurrence> {
    ensure!(actor > 0, IssueDetailError::Corrupt);
    ensure!(canonical_digest(issue_id), IssueDetailError::Corrupt);
    ensure!(canonical_digest(record_id), IssueDetailError::Corrupt);
    let authorization = authorization::identity(db, actor)?;
    let row = db
        .query_row(
            "SELECT i.project_id,o.project_id,o.shard_id,o.record_id,o.ingest_seq
             FROM issues i
             JOIN projects p ON p.id=i.project_id AND p.is_active=1
             JOIN issue_occurrences o ON o.issue_id=i.id
             WHERE i.id=?1 AND o.record_id=?2",
            params![issue_id, record_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()?
        .ok_or(IssueDetailError::NotFound)?;
    ensure!(
        row.0 > 0
            && row.0 == row.1
            && uuid::Uuid::parse_str(&row.2).is_ok_and(|shard| shard.to_string() == row.2)
            && row.3 == record_id
            && canonical_digest(&row.3)
            && row.4 > 0,
        IssueDetailError::Corrupt
    );
    Ok(AuthorizedOccurrence {
        authorization,
        location: OccurrenceLocation {
            issue_id: issue_id.to_owned(),
            project_id: row.0,
            shard_id: row.2,
            record_id: row.3,
            ingest_seq: row.4,
        },
    })
}

fn canonical_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
