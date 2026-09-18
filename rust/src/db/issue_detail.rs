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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_rejects_invalid_and_cross_project_occurrence_identity() {
        assert_eq!(IssueDetailError::NotFound.to_string(), "NotFound");
        assert_eq!(IssueDetailError::Corrupt.to_string(), "Corrupt");
        let directory = tempfile::tempdir().expect("temporary detail database");
        let db = crate::db::open(&directory.path().join("meta.db")).expect("open detail database");
        let issue = "a".repeat(64);
        let record = "b".repeat(64);
        assert!(lookup(&db, 0, &issue, &record).is_err());
        assert!(lookup(&db, 1, "invalid", &record).is_err());
        assert!(lookup(&db, 1, &issue, "invalid").is_err());

        let shard = uuid::Uuid::new_v4().to_string();
        db.execute_batch(
            "INSERT INTO users(id,email,password_hash,role,is_active,created_at_us,updated_at_us)
             VALUES(1,'admin@example.test','x','admin',1,0,0);
             INSERT INTO projects(id,slug,name,is_active,created_at_us,updated_at_us)
             VALUES(1,'one','One',1,0,0),(2,'two','Two',1,0,0);
             INSERT INTO runtime_state(singleton,installation_id,storage_generation,next_ingest_seq)
             VALUES(1,'installation','generation',1)",
        )
        .expect("seed principals and projects");
        db.execute(
            "INSERT INTO shards(id,schema_version,format_version,state,created_at_us)
             VALUES(?1,1,'test','local',0)",
            [&shard],
        )
        .expect("seed shard");
        db.execute(
            "INSERT INTO issues(id,project_id,fingerprint,fingerprint_version,title,level,status,
                 first_seen_us,last_seen_us,occurrence_count,first_seen_ingest_seq,
                 last_seen_ingest_seq,created_at_us,updated_at_us)
             VALUES(?1,1,'fingerprint',1,'title','error','unresolved',0,0,1,1,1,0,0)",
            [&issue],
        )
        .expect("seed issue");
        db.execute(
            "INSERT INTO issue_occurrences(event_key,project_id,issue_id,record_id,shard_id,
                 ingest_seq,occurred_at_us) VALUES(?1,2,?2,?3,?4,1,0)",
            params![record.clone(), issue, record.clone(), shard],
        )
        .expect("seed inconsistent occurrence");
        assert!(lookup(&db, 1, &"a".repeat(64), &record).is_err());
    }
}
