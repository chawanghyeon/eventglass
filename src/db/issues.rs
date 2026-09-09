//! Bounded Issue reads and atomic lifecycle updates.

use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::Serialize;

use crate::model::now_us;

#[derive(Debug)]
pub enum IssueDbError {
    Forbidden,
    NotFound,
    StaleRevision,
}

impl std::fmt::Display for IssueDbError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for IssueDbError {}

#[derive(Clone, Copy, Debug)]
pub enum IssueStatus {
    Unresolved,
    Resolved,
    Ignored,
}

impl IssueStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unresolved => "unresolved",
            Self::Resolved => "resolved",
            Self::Ignored => "ignored",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Issue {
    pub id: String,
    pub project_id: String,
    pub fingerprint: String,
    pub fingerprint_version: String,
    pub title: String,
    pub culprit: Option<String>,
    pub level: String,
    pub status: String,
    pub first_seen_us: String,
    pub last_seen_us: String,
    pub occurrence_count: String,
    pub first_release: Option<String>,
    pub last_release: Option<String>,
    pub resolved_at_us: Option<String>,
    pub resolved_through_ingest_seq: Option<String>,
    pub revision: String,
    pub created_at_us: String,
    pub updated_at_us: String,
}

#[derive(Debug, Serialize)]
pub struct IssueCursor {
    pub last_seen_us: String,
    pub id: String,
}

#[derive(Debug, Serialize)]
pub struct IssuePage {
    pub items: Vec<Issue>,
    pub next_cursor: Option<IssueCursor>,
}

#[derive(Debug, Serialize)]
pub struct Occurrence {
    pub record_id: String,
    pub source_event_id: Option<String>,
    pub ingest_seq: String,
    pub occurred_at_us: String,
}

#[derive(Debug, Serialize)]
pub struct OccurrenceCursor {
    pub occurred_at_us: String,
    pub ingest_seq: String,
}

#[derive(Debug, Serialize)]
pub struct OccurrencePage {
    pub items: Vec<Occurrence>,
    pub next_cursor: Option<OccurrenceCursor>,
}

fn require_actor(db: &Connection, actor: i64) -> Result<()> {
    let active: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM users WHERE id=?1 AND is_active=1)",
        [actor],
        |row| row.get(0),
    )?;
    if !active {
        return Err(IssueDbError::Forbidden.into());
    }
    Ok(())
}

fn require_project(db: &Connection, project_id: i64) -> Result<()> {
    let active: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE id=?1 AND is_active=1)",
        [project_id],
        |row| row.get(0),
    )?;
    if !active {
        return Err(IssueDbError::NotFound.into());
    }
    Ok(())
}

fn issue_from_row(row: &Row<'_>) -> rusqlite::Result<Issue> {
    Ok(Issue {
        id: row.get(0)?,
        project_id: row.get::<_, i64>(1)?.to_string(),
        fingerprint: row.get(2)?,
        fingerprint_version: row.get::<_, i64>(3)?.to_string(),
        title: row.get(4)?,
        culprit: row.get(5)?,
        level: row.get(6)?,
        status: row.get(7)?,
        first_seen_us: row.get::<_, i64>(8)?.to_string(),
        last_seen_us: row.get::<_, i64>(9)?.to_string(),
        occurrence_count: row.get::<_, i64>(10)?.to_string(),
        first_release: row.get(11)?,
        last_release: row.get(12)?,
        resolved_at_us: row
            .get::<_, Option<i64>>(13)?
            .map(|value| value.to_string()),
        resolved_through_ingest_seq: row
            .get::<_, Option<i64>>(14)?
            .map(|value| value.to_string()),
        revision: row.get::<_, i64>(15)?.to_string(),
        created_at_us: row.get::<_, i64>(16)?.to_string(),
        updated_at_us: row.get::<_, i64>(17)?.to_string(),
    })
}

const ISSUE_COLUMNS: &str = "i.id,i.project_id,i.fingerprint,i.fingerprint_version,i.title,i.culprit,i.level,\
     i.status,i.first_seen_us,i.last_seen_us,i.occurrence_count,i.first_release,i.last_release,\
     i.resolved_at_us,i.resolved_through_ingest_seq,i.revision,i.created_at_us,i.updated_at_us";

pub fn list(
    db: &Connection,
    actor: i64,
    project_id: i64,
    status: Option<&str>,
    query: Option<&str>,
    limit: usize,
    cursor: Option<(i64, &str)>,
) -> Result<IssuePage> {
    ensure!((1..=100).contains(&limit), "Issue page limit out of bounds");
    require_actor(db, actor)?;
    require_project(db, project_id)?;
    let fetch_limit = i64::try_from(limit.checked_add(1).expect("bounded limit cannot overflow"))?;
    let (cursor_time, cursor_id) = cursor
        .map(|(time, id)| (Some(time), Some(id)))
        .unwrap_or((None, None));
    let sql = format!(
        "SELECT {ISSUE_COLUMNS}
         FROM issues i
         WHERE i.project_id=?1
           AND (?2 IS NULL OR i.status=?2)
           AND (?6 IS NULL OR instr(lower(i.title), lower(?6)) > 0)
           AND (?3 IS NULL OR i.last_seen_us<?3 OR (i.last_seen_us=?3 AND i.id>?4))
         ORDER BY i.last_seen_us DESC,i.id ASC
         LIMIT ?5"
    );
    let mut statement = db.prepare(&sql)?;
    let mut items = statement
        .query_map(
            params![
                project_id,
                status,
                cursor_time,
                cursor_id,
                fetch_limit,
                query
            ],
            issue_from_row,
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let has_more = items.len() > limit;
    items.truncate(limit);
    let next_cursor = has_more.then(|| {
        let last = items.last().expect("positive bounded page has a last row");
        IssueCursor {
            last_seen_us: last.last_seen_us.clone(),
            id: last.id.clone(),
        }
    });
    Ok(IssuePage { items, next_cursor })
}

pub fn get(db: &Connection, actor: i64, issue_id: &str) -> Result<Issue> {
    require_actor(db, actor)?;
    let sql = format!(
        "SELECT {ISSUE_COLUMNS}
         FROM issues i JOIN projects p ON p.id=i.project_id
         WHERE i.id=?1 AND p.is_active=1"
    );
    db.query_row(&sql, [issue_id], issue_from_row)
        .optional()?
        .ok_or_else(|| IssueDbError::NotFound.into())
}

pub fn occurrences(
    db: &Connection,
    actor: i64,
    issue_id: &str,
    limit: usize,
    cursor: Option<(i64, i64)>,
) -> Result<OccurrencePage> {
    ensure!(
        (1..=100).contains(&limit),
        "occurrence page limit out of bounds"
    );
    // The same active-project check used by detail also avoids leaking whether a
    // disabled project's Issue exists through an empty occurrence page.
    let _ = get(db, actor, issue_id)?;
    let (cursor_time, cursor_seq) = cursor
        .map(|(time, seq)| (Some(time), Some(seq)))
        .unwrap_or((None, None));
    let fetch_limit = i64::try_from(limit.checked_add(1).expect("bounded limit cannot overflow"))?;
    let mut statement = db.prepare(
        "SELECT record_id,source_event_id,ingest_seq,occurred_at_us
         FROM issue_occurrences
         WHERE issue_id=?1
           AND (?2 IS NULL OR occurred_at_us<?2 OR (occurred_at_us=?2 AND ingest_seq<?3))
         ORDER BY occurred_at_us DESC,ingest_seq DESC
         LIMIT ?4",
    )?;
    let mut items = statement
        .query_map(
            params![issue_id, cursor_time, cursor_seq, fetch_limit],
            |row| {
                Ok(Occurrence {
                    record_id: row.get(0)?,
                    source_event_id: row.get(1)?,
                    ingest_seq: row.get::<_, i64>(2)?.to_string(),
                    occurred_at_us: row.get::<_, i64>(3)?.to_string(),
                })
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let has_more = items.len() > limit;
    items.truncate(limit);
    let next_cursor = has_more.then(|| {
        let last = items.last().expect("positive bounded page has a last row");
        OccurrenceCursor {
            occurred_at_us: last.occurred_at_us.clone(),
            ingest_seq: last.ingest_seq.clone(),
        }
    });
    Ok(OccurrencePage { items, next_cursor })
}

pub fn set_status(
    db: &mut Connection,
    actor: i64,
    issue_id: &str,
    status: IssueStatus,
    expected_revision: i64,
) -> Result<Issue> {
    let tx = db.transaction()?;
    require_actor(&tx, actor)?;
    let current_revision: Option<i64> = tx
        .query_row(
            "SELECT i.revision
             FROM issues i JOIN projects p ON p.id=i.project_id
             WHERE i.id=?1 AND p.is_active=1",
            [issue_id],
            |row| row.get(0),
        )
        .optional()?;
    let current_revision = current_revision.ok_or(IssueDbError::NotFound)?;
    if current_revision != expected_revision {
        return Err(IssueDbError::StaleRevision.into());
    }

    let now = now_us()?;
    let resolved_through = if matches!(status, IssueStatus::Resolved) {
        Some(tx.query_row(
            "SELECT next_ingest_seq-1 FROM runtime_state WHERE singleton=1",
            [],
            |row| row.get::<_, i64>(0),
        )?)
    } else {
        None
    };
    if tx.execute(
        "UPDATE issues SET status=?1,revision=revision+1,updated_at_us=?2,
             resolved_at_us=?3,resolved_through_ingest_seq=?4
         WHERE id=?5 AND revision=?6",
        params![
            status.as_str(),
            now,
            matches!(status, IssueStatus::Resolved).then_some(now),
            resolved_through,
            issue_id,
            expected_revision
        ],
    )? != 1
    {
        return Err(IssueDbError::StaleRevision.into());
    }
    let issue = get(&tx, actor, issue_id)?;
    tx.commit()?;
    Ok(issue)
}
