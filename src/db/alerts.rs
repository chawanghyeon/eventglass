//! Alert administration and durable outbox state transitions.

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use sha2::Digest;

use crate::alerts::{Condition, Configuration, Destination};

#[derive(Debug, Clone)]
pub struct DueDelivery {
    pub id: String,
    pub payload_json: String,
    pub attempts: u32,
}

#[derive(Debug)]
pub enum DeliveryResult {
    Sent {
        status: u16,
    },
    Retry {
        status: Option<u16>,
        next_retry_at_us: i64,
        error: &'static str,
    },
    Failed {
        status: Option<u16>,
        error: &'static str,
    },
}

#[derive(Debug, Clone)]
pub struct PendingEvaluation {
    pub alert_id: i64,
    pub revision: i64,
    pub project_ids: Vec<i64>,
    pub project_id: Option<i64>,
    pub condition: Condition,
    pub destination: Destination,
    pub evaluation_end_us: i64,
    pub cut_seq: i64,
    pub last_triggered_at_us: Option<i64>,
}

type EvaluationRow = (i64, i64, Option<i64>, String, String, i64, i64, Option<i64>);

#[derive(Debug)]
pub enum AlertError {
    Forbidden,
    Invalid,
    NotFound,
    RevisionConflict,
}

impl std::fmt::Display for AlertError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl std::error::Error for AlertError {}

#[derive(Debug, Serialize)]
pub struct Alert {
    pub id: String,
    pub name: String,
    pub project_id: Option<String>,
    pub revision: i64,
    pub condition: Condition,
    pub destination: Destination,
    pub enabled: bool,
    pub last_evaluation_error: Option<String>,
    pub last_evaluation_watermark: Option<String>,
    pub pending_evaluation_end_us: Option<String>,
    pub pending_cut_seq: Option<String>,
    pub last_evaluated_at_us: Option<String>,
    pub last_triggered_at_us: Option<String>,
    pub created_at_us: String,
    pub updated_at_us: String,
}

#[derive(Debug, Serialize)]
pub struct Delivery {
    pub id: String,
    pub alert_id: String,
    pub payload: serde_json::Value,
    pub state: String,
    pub sent_at_us: Option<String>,
    pub last_status_code: Option<u16>,
    pub attempts: u32,
    pub next_retry_at_us: String,
    pub created_at_us: String,
    pub last_error: Option<String>,
}

fn require_admin(db: &Connection, actor: i64) -> Result<()> {
    let permitted: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM users WHERE id=?1 AND role='admin' AND is_active=1)",
        [actor],
        |row| row.get(0),
    )?;
    if !permitted {
        return Err(AlertError::Forbidden.into());
    }
    Ok(())
}

fn decode_alert(row: &rusqlite::Row<'_>) -> rusqlite::Result<Alert> {
    let condition_json: String = row.get(4)?;
    let destination_json: String = row.get(5)?;
    let condition = serde_json::from_str(&condition_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            condition_json.len(),
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })?;
    let destination = serde_json::from_str(&destination_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            destination_json.len(),
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })?;
    Ok(Alert {
        id: row.get::<_, i64>(0)?.to_string(),
        name: row.get(1)?,
        project_id: row.get::<_, Option<i64>>(2)?.map(|value| value.to_string()),
        revision: row.get(3)?,
        condition,
        destination,
        enabled: row.get(6)?,
        last_evaluation_error: row.get(7)?,
        last_evaluation_watermark: row.get::<_, Option<i64>>(8)?.map(|v| v.to_string()),
        pending_evaluation_end_us: row.get::<_, Option<i64>>(9)?.map(|v| v.to_string()),
        pending_cut_seq: row.get::<_, Option<i64>>(10)?.map(|v| v.to_string()),
        last_evaluated_at_us: row.get::<_, Option<i64>>(11)?.map(|v| v.to_string()),
        last_triggered_at_us: row.get::<_, Option<i64>>(12)?.map(|v| v.to_string()),
        created_at_us: row.get::<_, i64>(13)?.to_string(),
        updated_at_us: row.get::<_, i64>(14)?.to_string(),
    })
}

const ALERT_SELECT: &str = "SELECT id,name,project_id,revision,condition_json,destination_json,
    enabled,last_evaluation_error,last_evaluation_watermark,pending_evaluation_end_us,
    pending_cut_seq,last_evaluated_at_us,last_triggered_at_us,created_at_us,updated_at_us
    FROM alerts";

pub fn list(db: &Connection, actor: i64) -> Result<Vec<Alert>> {
    require_admin(db, actor)?;
    let mut statement = db.prepare(&format!(
        "{ALERT_SELECT} WHERE deleted_at_us IS NULL ORDER BY id"
    ))?;
    Ok(statement
        .query_map([], decode_alert)?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn create(
    db: &mut Connection,
    actor: i64,
    configuration: &Configuration,
    now_us: i64,
) -> Result<i64> {
    configuration.validate().map_err(|_| AlertError::Invalid)?;
    let tx = db.transaction()?;
    require_admin(&tx, actor)?;
    if let Some(project) = configuration.project_id {
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM projects WHERE id=?1 AND is_active=1)",
            [project],
            |row| row.get(0),
        )?;
        ensure!(exists, AlertError::Invalid);
    }
    tx.execute(
        "INSERT INTO alerts(project_id,name,condition_type,condition_json,destination_type,
             destination_json,enabled,created_at_us,updated_at_us)
         VALUES(?1,?2,?3,?4,'webhook',?5,?6,?7,?7)",
        params![
            configuration.project_id,
            configuration.name.trim(),
            configuration.condition.kind(),
            serde_json::to_string(&configuration.condition)?,
            serde_json::to_string(&configuration.destination)?,
            configuration.enabled,
            now_us,
        ],
    )?;
    let id = tx.last_insert_rowid();
    tx.commit()?;
    Ok(id)
}

pub fn update(
    db: &mut Connection,
    actor: i64,
    id: i64,
    expected_revision: i64,
    configuration: &Configuration,
    now_us: i64,
) -> Result<Alert> {
    configuration.validate().map_err(|_| AlertError::Invalid)?;
    if expected_revision < 0 {
        return Err(AlertError::Invalid.into());
    }
    let tx = db.transaction()?;
    require_admin(&tx, actor)?;
    if let Some(project) = configuration.project_id {
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM projects WHERE id=?1 AND is_active=1)",
            [project],
            |row| row.get(0),
        )?;
        ensure!(exists, AlertError::Invalid);
    }
    if tx.execute(
        "UPDATE alerts SET project_id=?1,name=?2,condition_type=?3,condition_json=?4,
             destination_type='webhook',destination_json=?5,enabled=?6,revision=revision+1,
             pending_evaluation_end_us=NULL,pending_cut_seq=NULL,last_evaluation_error=NULL,
             updated_at_us=?7
         WHERE id=?8 AND revision=?9 AND deleted_at_us IS NULL",
        params![
            configuration.project_id,
            configuration.name.trim(),
            configuration.condition.kind(),
            serde_json::to_string(&configuration.condition)?,
            serde_json::to_string(&configuration.destination)?,
            configuration.enabled,
            now_us,
            id,
            expected_revision,
        ],
    )? == 0
    {
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM alerts WHERE id=?1 AND deleted_at_us IS NULL)",
            [id],
            |row| row.get(0),
        )?;
        return Err(if exists {
            AlertError::RevisionConflict
        } else {
            AlertError::NotFound
        }
        .into());
    }
    tx.execute(
        "UPDATE alert_deliveries SET state='cancelled',last_error='alert_reconfigured'
         WHERE alert_id=?1 AND state='pending'",
        [id],
    )?;
    let alert = tx.query_row(&format!("{ALERT_SELECT} WHERE id=?1"), [id], decode_alert)?;
    tx.commit()?;
    Ok(alert)
}

pub fn delete(
    db: &mut Connection,
    actor: i64,
    id: i64,
    expected_revision: i64,
    now_us: i64,
) -> Result<()> {
    let tx = db.transaction()?;
    require_admin(&tx, actor)?;
    if tx.execute(
        "UPDATE alerts SET deleted_at_us=?1,enabled=0,revision=revision+1,
             pending_evaluation_end_us=NULL,pending_cut_seq=NULL,updated_at_us=?1
         WHERE id=?2 AND revision=?3 AND deleted_at_us IS NULL",
        params![now_us, id, expected_revision],
    )? == 0
    {
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM alerts WHERE id=?1 AND deleted_at_us IS NULL)",
            [id],
            |row| row.get(0),
        )?;
        return Err(if exists {
            AlertError::RevisionConflict
        } else {
            AlertError::NotFound
        }
        .into());
    }
    tx.execute(
        "UPDATE alert_deliveries SET state='cancelled',last_error='alert_deleted'
         WHERE alert_id=?1 AND state='pending'",
        [id],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn deliveries(db: &Connection, actor: i64, limit: usize) -> Result<Vec<Delivery>> {
    require_admin(db, actor)?;
    if !(1..=500).contains(&limit) {
        return Err(AlertError::Invalid.into());
    }
    let mut statement = db.prepare(
        "SELECT id,alert_id,payload_json,state,sent_at_us,last_status_code,attempts,
             next_retry_at_us,created_at_us,last_error
         FROM alert_deliveries ORDER BY created_at_us DESC,id DESC LIMIT ?1",
    )?;
    Ok(statement
        .query_map([i64::try_from(limit)?], |row| {
            let raw: String = row.get(2)?;
            let payload = serde_json::from_str(&raw).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    raw.len(),
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            Ok(Delivery {
                id: row.get(0)?,
                alert_id: row.get::<_, i64>(1)?.to_string(),
                payload,
                state: row.get(3)?,
                sent_at_us: row.get::<_, Option<i64>>(4)?.map(|v| v.to_string()),
                last_status_code: row.get::<_, Option<u16>>(5)?,
                attempts: row.get(6)?,
                next_retry_at_us: row.get::<_, i64>(7)?.to_string(),
                created_at_us: row.get::<_, i64>(8)?.to_string(),
                last_error: row.get(9)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn retry(db: &mut Connection, actor: i64, id: &str, now_us: i64) -> Result<()> {
    let tx = db.transaction()?;
    require_admin(&tx, actor)?;
    if tx.execute(
        "UPDATE alert_deliveries SET state='pending',attempts=0,next_retry_at_us=?1,
             last_error=NULL,last_status_code=NULL
         WHERE id=?2 AND state='failed'
           AND EXISTS(SELECT 1 FROM alerts WHERE alerts.id=alert_deliveries.alert_id
             AND alerts.deleted_at_us IS NULL AND alerts.enabled=1)",
        params![now_us, id],
    )? == 0
    {
        return Err(AlertError::NotFound.into());
    }
    tx.commit()?;
    Ok(())
}

pub fn get(db: &Connection, actor: i64, id: i64) -> Result<Option<Alert>> {
    require_admin(db, actor)?;
    Ok(db
        .query_row(
            &format!("{ALERT_SELECT} WHERE id=?1 AND deleted_at_us IS NULL"),
            [id],
            decode_alert,
        )
        .optional()?)
}

pub fn due_delivery(db: &Connection, now_us: i64) -> Result<Option<DueDelivery>> {
    Ok(db
        .query_row(
            "SELECT deliveries.id,deliveries.payload_json,deliveries.attempts
             FROM alert_deliveries AS deliveries
             JOIN alerts ON alerts.id=deliveries.alert_id
             WHERE deliveries.state='pending' AND deliveries.next_retry_at_us<=?1
               AND alerts.enabled=1 AND alerts.deleted_at_us IS NULL
             ORDER BY deliveries.next_retry_at_us,deliveries.created_at_us,deliveries.id
             LIMIT 1",
            [now_us],
            |row| {
                Ok(DueDelivery {
                    id: row.get(0)?,
                    payload_json: row.get(1)?,
                    attempts: row.get(2)?,
                })
            },
        )
        .optional()?)
}

pub fn finish_delivery(
    db: &Connection,
    delivery: &DueDelivery,
    result: DeliveryResult,
    now_us: i64,
) -> Result<bool> {
    let attempts = delivery
        .attempts
        .checked_add(1)
        .ok_or(AlertError::Invalid)?;
    let changed = match result {
        DeliveryResult::Sent { status } => db.execute(
            "UPDATE alert_deliveries SET state='sent',sent_at_us=?1,last_status_code=?2,
                 attempts=?3,last_error=NULL
             WHERE id=?4 AND state='pending' AND attempts=?5",
            params![now_us, status, attempts, delivery.id, delivery.attempts],
        )?,
        DeliveryResult::Retry {
            status,
            next_retry_at_us,
            error,
        } => db.execute(
            "UPDATE alert_deliveries SET next_retry_at_us=?1,last_status_code=?2,
                 attempts=?3,last_error=?4
             WHERE id=?5 AND state='pending' AND attempts=?6",
            params![
                next_retry_at_us,
                status,
                attempts,
                error,
                delivery.id,
                delivery.attempts
            ],
        )?,
        DeliveryResult::Failed { status, error } => db.execute(
            "UPDATE alert_deliveries SET state='failed',last_status_code=?1,
                 attempts=?2,last_error=?3
             WHERE id=?4 AND state='pending' AND attempts=?5",
            params![status, attempts, error, delivery.id, delivery.attempts],
        )?,
    };
    Ok(changed == 1)
}

pub fn reserve_evaluation(db: &mut Connection, now_us: i64) -> Result<Option<PendingEvaluation>> {
    const MINUTE_US: i64 = 60_000_000;
    let tx = db.transaction()?;
    let pending: Option<EvaluationRow> = tx
        .query_row(
            "SELECT id,revision,project_id,condition_json,destination_json,
                 pending_evaluation_end_us,pending_cut_seq,last_triggered_at_us
             FROM alerts WHERE enabled=1 AND deleted_at_us IS NULL
               AND condition_type IN ('error_count','log_count')
               AND pending_evaluation_end_us IS NOT NULL
             ORDER BY pending_evaluation_end_us,id LIMIT 1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .optional()?;
    let row = if let Some(pending) = pending {
        pending
    } else {
        let mut statement = tx.prepare(
            "SELECT id,revision,project_id,condition_json,destination_json,
                 last_evaluated_at_us,created_at_us,last_triggered_at_us
             FROM alerts WHERE enabled=1 AND deleted_at_us IS NULL
               AND condition_type IN ('error_count','log_count')
               AND pending_evaluation_end_us IS NULL
             ORDER BY id LIMIT 1001",
        )?;
        let candidates = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        if candidates.len() > 1_000 {
            anyhow::bail!("enabled threshold alert count exceeds evaluation bound");
        }
        let mut due = None;
        for (id, revision, project, condition, destination, last, created, triggered) in candidates
        {
            let end = match last {
                Some(last) => last.checked_add(MINUTE_US),
                None => created
                    .checked_div(MINUTE_US)
                    .and_then(|minute| minute.checked_add(1))
                    .and_then(|minute| minute.checked_mul(MINUTE_US)),
            }
            .context("alert evaluation time overflow")?;
            if end <= now_us
                && due
                    .as_ref()
                    .is_none_or(|(_, _, _, _, _, current, _, _)| end < *current)
            {
                due = Some((
                    id,
                    revision,
                    project,
                    condition,
                    destination,
                    end,
                    0,
                    triggered,
                ));
            }
        }
        let Some(mut due) = due else {
            tx.commit()?;
            return Ok(None);
        };
        due.6 = tx.query_row(
            "SELECT next_ingest_seq-1 FROM runtime_state WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        if tx.execute(
            "UPDATE alerts SET pending_evaluation_end_us=?1,pending_cut_seq=?2
             WHERE id=?3 AND revision=?4 AND pending_evaluation_end_us IS NULL",
            params![due.5, due.6, due.0, due.1],
        )? != 1
        {
            anyhow::bail!("alert changed while reserving evaluation");
        }
        due
    };
    let condition: Condition = serde_json::from_str(&row.3)?;
    let destination: Destination = serde_json::from_str(&row.4)?;
    let project_ids = if let Some(project_id) = row.2 {
        let active: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM projects WHERE id=?1 AND is_active=1)",
            [project_id],
            |result| result.get(0),
        )?;
        if !active {
            Vec::new()
        } else {
            vec![project_id]
        }
    } else {
        let mut statement =
            tx.prepare("SELECT id FROM projects WHERE is_active=1 ORDER BY id LIMIT 1001")?;
        let projects = statement
            .query_map([], |result| result.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        if projects.len() > 1_000 {
            anyhow::bail!("alert project scope exceeds evaluation bound");
        }
        projects
    };
    tx.commit()?;
    Ok(Some(PendingEvaluation {
        alert_id: row.0,
        revision: row.1,
        project_ids,
        project_id: row.2,
        condition,
        destination,
        evaluation_end_us: row.5,
        cut_seq: row.6,
        last_triggered_at_us: row.7,
    }))
}

pub fn finish_evaluation(
    db: &mut Connection,
    pending: &PendingEvaluation,
    count: u64,
    now_us: i64,
) -> Result<bool> {
    let (threshold, cooldown_seconds) = match &pending.condition {
        Condition::ErrorCount {
            threshold,
            cooldown_seconds,
            ..
        }
        | Condition::LogCount {
            threshold,
            cooldown_seconds,
            ..
        } => (*threshold, *cooldown_seconds),
        _ => return Err(AlertError::Invalid.into()),
    };
    let tx = db.transaction()?;
    let unchanged: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM alerts WHERE id=?1 AND revision=?2 AND enabled=1
             AND deleted_at_us IS NULL AND pending_evaluation_end_us=?3 AND pending_cut_seq=?4)",
        params![
            pending.alert_id,
            pending.revision,
            pending.evaluation_end_us,
            pending.cut_seq
        ],
        |row| row.get(0),
    )?;
    if !unchanged {
        tx.commit()?;
        return Ok(false);
    }
    let cooldown_us = i64::from(cooldown_seconds).saturating_mul(1_000_000);
    let outside_cooldown = pending
        .last_triggered_at_us
        .is_none_or(|last| last <= pending.evaluation_end_us.saturating_sub(cooldown_us));
    if count >= threshold && outside_cooldown {
        let dedupe_key = format!(
            "eventglass:{}:r{}:threshold:{}",
            pending.alert_id, pending.revision, pending.evaluation_end_us
        );
        let delivery_id = format!(
            "{:x}",
            sha2::Sha256::digest(format!("eventglass.alert.delivery.v1:{dedupe_key}").as_bytes())
        );
        let payload = serde_json::json!({
            "version":1,
            "kind":"threshold",
            "alert_id":pending.alert_id.to_string(),
            "alert_revision":pending.revision,
            "evaluation_end_us":pending.evaluation_end_us.to_string(),
            "cut_ingest_seq":pending.cut_seq.to_string(),
            "count":count.to_string(),
            "destination":pending.destination.clone(),
        });
        tx.execute(
            "INSERT INTO alert_deliveries(id,alert_id,dedupe_key,payload_json,state,attempts,
                 next_retry_at_us,created_at_us)
             VALUES(?1,?2,?3,?4,'pending',0,?5,?5)
             ON CONFLICT(dedupe_key) DO NOTHING",
            params![
                delivery_id,
                pending.alert_id,
                dedupe_key,
                payload.to_string(),
                now_us
            ],
        )?;
    }
    let triggered = count >= threshold && outside_cooldown;
    if tx.execute(
        "UPDATE alerts SET last_evaluated_at_us=?1,last_evaluation_watermark=?2,
             pending_evaluation_end_us=NULL,pending_cut_seq=NULL,last_evaluation_error=NULL,
             last_triggered_at_us=CASE WHEN ?3 THEN ?1 ELSE last_triggered_at_us END
         WHERE id=?4 AND revision=?5 AND pending_evaluation_end_us=?1 AND pending_cut_seq=?2",
        params![
            pending.evaluation_end_us,
            pending.cut_seq,
            triggered,
            pending.alert_id,
            pending.revision
        ],
    )? != 1
    {
        anyhow::bail!("alert changed while completing evaluation");
    }
    tx.commit()?;
    Ok(true)
}

pub fn fail_evaluation(
    db: &Connection,
    pending: &PendingEvaluation,
    error: &'static str,
) -> Result<bool> {
    Ok(db.execute(
        "UPDATE alerts SET last_evaluation_error=?1
         WHERE id=?2 AND revision=?3 AND pending_evaluation_end_us=?4 AND pending_cut_seq=?5",
        params![
            error,
            pending.alert_id,
            pending.revision,
            pending.evaluation_end_us,
            pending.cut_seq
        ],
    )? == 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerts::TimeBasis;

    fn database() -> (tempfile::TempDir, Connection) {
        let directory = tempfile::tempdir().unwrap();
        let db = crate::db::open(&directory.path().join("meta.db")).unwrap();
        db.execute_batch(
            "INSERT INTO users(id,email,password_hash,role,is_active,created_at_us,updated_at_us)
             VALUES(1,'admin@example.test','x','admin',1,0,0),
                   (2,'member@example.test','x','member',1,0,0);
             INSERT INTO projects(id,slug,name,is_active,created_at_us,updated_at_us)
             VALUES(1,'one','One',1,0,0);
             INSERT INTO runtime_state(singleton,installation_id,storage_generation,next_ingest_seq)
             VALUES(1,'test-installation','test-generation',1)",
        )
        .unwrap();
        (directory, db)
    }

    fn configuration() -> Configuration {
        Configuration {
            name: "Errors".into(),
            project_id: Some(1),
            condition: Condition::ErrorCount {
                query: String::new(),
                window_seconds: 60,
                threshold: 1,
                cooldown_seconds: 0,
                time_basis: TimeBasis::ReceivedAt,
            },
            destination: Destination::Webhook {
                url: "https://example.test/hook".into(),
            },
            enabled: true,
        }
    }

    fn assert_alert_error(error: anyhow::Error, expected: AlertError) {
        let actual = error.downcast_ref::<AlertError>().unwrap();
        assert_eq!(actual.to_string(), expected.to_string());
        assert!(std::error::Error::source(actual).is_none());
    }

    #[test]
    fn administrative_failures_distinguish_invalid_forbidden_missing_and_conflict() {
        let (_directory, mut db) = database();
        assert_alert_error(list(&db, 2).unwrap_err(), AlertError::Forbidden);

        let mut invalid = configuration();
        invalid.name.clear();
        assert_alert_error(
            create(&mut db, 1, &invalid, 1).unwrap_err(),
            AlertError::Invalid,
        );
        let mut unknown_project = configuration();
        unknown_project.project_id = Some(99);
        assert_alert_error(
            create(&mut db, 1, &unknown_project, 1).unwrap_err(),
            AlertError::Invalid,
        );

        let id = create(&mut db, 1, &configuration(), 1).unwrap();
        assert!(get(&db, 1, id).unwrap().is_some());
        assert!(get(&db, 1, 999).unwrap().is_none());
        assert_alert_error(
            update(&mut db, 1, id, -1, &configuration(), 2).unwrap_err(),
            AlertError::Invalid,
        );
        assert_alert_error(
            update(&mut db, 1, id, 99, &configuration(), 2).unwrap_err(),
            AlertError::RevisionConflict,
        );
        assert_alert_error(
            update(&mut db, 1, 999, 0, &configuration(), 2).unwrap_err(),
            AlertError::NotFound,
        );
        assert_alert_error(
            update(&mut db, 1, id, 0, &unknown_project, 2).unwrap_err(),
            AlertError::Invalid,
        );
        assert_alert_error(
            delete(&mut db, 1, id, 99, 2).unwrap_err(),
            AlertError::RevisionConflict,
        );
        assert_alert_error(
            delete(&mut db, 1, 999, 0, 2).unwrap_err(),
            AlertError::NotFound,
        );
        for limit in [0, 501] {
            assert_alert_error(deliveries(&db, 1, limit).unwrap_err(), AlertError::Invalid);
        }
        assert_alert_error(
            retry(&mut db, 1, "missing", 3).unwrap_err(),
            AlertError::NotFound,
        );
    }

    #[test]
    fn administrative_conflict_probes_propagate_sqlite_authorization_failures() {
        use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let (_directory, mut update_db) = database();
        let id = create(&mut update_db, 1, &configuration(), 1).expect("create update alert");
        let selects = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&selects);
        update_db
            .authorizer(Some(move |context: AuthContext<'_>| {
                if matches!(context.action, AuthAction::Select)
                    && observed.fetch_add(1, Ordering::SeqCst) == 2
                {
                    Authorization::Deny
                } else {
                    Authorization::Allow
                }
            }))
            .expect("install update authorizer");
        assert!(update(&mut update_db, 1, id, 99, &configuration(), 2).is_err());

        let (_directory, mut delete_db) = database();
        let id = create(&mut delete_db, 1, &configuration(), 1).expect("create delete alert");
        let selects = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&selects);
        delete_db
            .authorizer(Some(move |context: AuthContext<'_>| {
                if matches!(context.action, AuthAction::Select)
                    && observed.fetch_add(1, Ordering::SeqCst) == 1
                {
                    Authorization::Deny
                } else {
                    Authorization::Allow
                }
            }))
            .expect("install delete authorizer");
        assert!(delete(&mut delete_db, 1, id, 99, 2).is_err());
    }

    #[test]
    fn corrupt_alert_and_delivery_json_fail_closed() {
        let (_directory, mut db) = database();
        let id = create(&mut db, 1, &configuration(), 1).unwrap();
        db.execute("UPDATE alerts SET condition_json='{' WHERE id=?1", [id])
            .unwrap();
        assert!(list(&db, 1).is_err());
        db.execute(
            "UPDATE alerts SET condition_json=?1,destination_json='{' WHERE id=?2",
            params![
                serde_json::to_string(&configuration().condition).unwrap(),
                id
            ],
        )
        .unwrap();
        assert!(get(&db, 1, id).is_err());

        db.execute(
            "INSERT INTO alert_deliveries(id,alert_id,dedupe_key,payload_json,state,attempts,
                 next_retry_at_us,created_at_us)
             VALUES('delivery',?1,'dedupe','{','pending',0,0,0)",
            [id],
        )
        .unwrap();
        assert!(deliveries(&db, 1, 10).is_err());
    }

    #[test]
    fn delivery_completion_covers_sent_retry_failed_stale_and_overflow() {
        let (_directory, mut db) = database();
        let alert_id = create(&mut db, 1, &configuration(), 1).unwrap();
        for id in ["sent", "retry", "failed"] {
            db.execute(
                "INSERT INTO alert_deliveries(id,alert_id,dedupe_key,payload_json,state,attempts,
                     next_retry_at_us,created_at_us)
                 VALUES(?1,?2,?1,'{}','pending',0,0,0)",
                params![id, alert_id],
            )
            .unwrap();
        }
        let due = due_delivery(&db, 0).unwrap().unwrap();
        assert_eq!(due.attempts, 0);

        let sent = DueDelivery {
            id: "sent".into(),
            payload_json: "{}".into(),
            attempts: 0,
        };
        assert!(finish_delivery(&db, &sent, DeliveryResult::Sent { status: 204 }, 10).unwrap());
        assert!(!finish_delivery(&db, &sent, DeliveryResult::Sent { status: 204 }, 11).unwrap());

        let retrying = DueDelivery {
            id: "retry".into(),
            payload_json: "{}".into(),
            attempts: 0,
        };
        assert!(
            finish_delivery(
                &db,
                &retrying,
                DeliveryResult::Retry {
                    status: Some(429),
                    next_retry_at_us: 99,
                    error: "retry"
                },
                10
            )
            .unwrap()
        );

        let failed = DueDelivery {
            id: "failed".into(),
            payload_json: "{}".into(),
            attempts: 0,
        };
        assert!(
            finish_delivery(
                &db,
                &failed,
                DeliveryResult::Failed {
                    status: None,
                    error: "failed"
                },
                10
            )
            .unwrap()
        );
        let overflow = DueDelivery {
            id: "missing".into(),
            payload_json: "{}".into(),
            attempts: u32::MAX,
        };
        assert_alert_error(
            finish_delivery(&db, &overflow, DeliveryResult::Sent { status: 200 }, 10).unwrap_err(),
            AlertError::Invalid,
        );
    }

    #[test]
    fn threshold_reservation_covers_scopes_no_trigger_failure_and_stale_completion() {
        let (_directory, mut db) = database();
        let id = create(&mut db, 1, &configuration(), 0).unwrap();
        let pending = reserve_evaluation(&mut db, 60_000_000).unwrap().unwrap();
        assert_eq!(pending.alert_id, id);
        assert_eq!(pending.project_ids, vec![1]);
        assert!(finish_evaluation(&mut db, &pending, 0, 60_000_000).unwrap());

        let pending = reserve_evaluation(&mut db, 120_000_000).unwrap().unwrap();
        assert!(fail_evaluation(&db, &pending, "query_incomplete").unwrap());
        let mut stale = pending.clone();
        stale.revision += 1;
        assert!(!fail_evaluation(&db, &stale, "query_incomplete").unwrap());
        assert!(!finish_evaluation(&mut db, &stale, 1, 120_000_000).unwrap());

        db.execute(
            "UPDATE alerts SET enabled=0,pending_evaluation_end_us=NULL,pending_cut_seq=NULL WHERE id=?1",
            [id],
        )
        .unwrap();
        let inactive_id = create(&mut db, 1, &configuration(), 0).unwrap();
        db.execute("UPDATE projects SET is_active=0 WHERE id=1", [])
            .unwrap();
        let inactive = reserve_evaluation(&mut db, 60_000_000).unwrap().unwrap();
        assert_eq!(inactive.alert_id, inactive_id);
        assert!(inactive.project_ids.is_empty());
        assert!(finish_evaluation(&mut db, &inactive, 0, 60_000_000).unwrap());
        db.execute("UPDATE projects SET is_active=1 WHERE id=1", [])
            .unwrap();
        let mut global = configuration();
        global.project_id = None;
        let global_id = create(&mut db, 1, &global, 0).unwrap();
        let pending = reserve_evaluation(&mut db, 60_000_000).unwrap().unwrap();
        assert_eq!(pending.alert_id, global_id);
        assert_eq!(pending.project_ids, vec![1]);

        let invalid = PendingEvaluation {
            alert_id: global_id,
            revision: 0,
            project_ids: vec![1],
            project_id: None,
            condition: Condition::NewIssue,
            destination: global.destination,
            evaluation_end_us: 60_000_000,
            cut_seq: 0,
            last_triggered_at_us: None,
        };
        assert_alert_error(
            finish_evaluation(&mut db, &invalid, 1, 60_000_000).unwrap_err(),
            AlertError::Invalid,
        );

        let mut log_pending = pending;
        log_pending.condition = Condition::LogCount {
            query: String::new(),
            window_seconds: 60,
            threshold: 1,
            cooldown_seconds: 0,
            time_basis: TimeBasis::Timestamp,
        };
        assert!(finish_evaluation(&mut db, &log_pending, 0, 60_000_000).unwrap());
    }

    #[test]
    fn damaged_administrative_tables_propagate_every_write_failure() {
        let missing_users = Connection::open_in_memory().expect("missing users database");
        assert!(require_admin(&missing_users, 1).is_err());

        let (_directory, mut missing_projects) = database();
        missing_projects
            .execute_batch("PRAGMA foreign_keys=OFF; DROP TABLE projects")
            .expect("drop projects");
        assert!(create(&mut missing_projects, 1, &configuration(), 1).is_err());

        let (_directory, mut missing_alerts) = database();
        missing_alerts
            .execute_batch(
                "PRAGMA foreign_keys=OFF; DROP TABLE alert_deliveries; DROP TABLE alerts",
            )
            .expect("drop alerts");
        assert!(create(&mut missing_alerts, 1, &configuration(), 1).is_err());
        assert!(deliveries(&missing_alerts, 1, 1).is_err());
        let delivery = DueDelivery {
            id: "missing".into(),
            payload_json: "{}".into(),
            attempts: 0,
        };
        for result in [
            DeliveryResult::Sent { status: 200 },
            DeliveryResult::Retry {
                status: None,
                next_retry_at_us: 1,
                error: "retry",
            },
            DeliveryResult::Failed {
                status: None,
                error: "failed",
            },
        ] {
            assert!(finish_delivery(&missing_alerts, &delivery, result, 1).is_err());
        }

        let pending = PendingEvaluation {
            alert_id: 1,
            revision: 0,
            project_ids: vec![1],
            project_id: Some(1),
            condition: configuration().condition,
            destination: configuration().destination,
            evaluation_end_us: 1,
            cut_seq: 0,
            last_triggered_at_us: None,
        };
        assert!(finish_evaluation(&mut missing_alerts, &pending, 0, 1).is_err());
        assert!(fail_evaluation(&missing_alerts, &pending, "failure").is_err());
    }

    #[test]
    fn threshold_bounds_and_revision_triggers_fail_closed() {
        let (_directory, mut missing_projects) = database();
        let id = create(&mut missing_projects, 1, &configuration(), 0).expect("alert");
        missing_projects
            .execute_batch("PRAGMA foreign_keys=OFF; DROP TABLE projects")
            .expect("drop projects");
        assert!(update(&mut missing_projects, 1, id, 0, &configuration(), 1).is_err());

        let (_directory, mut missing_alerts) = database();
        missing_alerts
            .execute_batch(
                "PRAGMA foreign_keys=OFF; DROP TABLE alert_deliveries; DROP TABLE alerts",
            )
            .expect("drop alert tables");
        assert!(update(&mut missing_alerts, 1, 1, 0, &configuration(), 1).is_err());
        assert!(delete(&mut missing_alerts, 1, 1, 0, 1).is_err());

        let (_directory, mut too_many_alerts) = database();
        let condition = serde_json::to_string(&configuration().condition).expect("condition JSON");
        let destination =
            serde_json::to_string(&configuration().destination).expect("destination JSON");
        too_many_alerts
            .execute(
                "WITH RECURSIVE ids(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM ids WHERE value<1001)
                 INSERT INTO alerts(
                    id,name,condition_type,condition_json,destination_type,destination_json,
                    enabled,created_at_us,updated_at_us)
                 SELECT value,'bulk','error_count',?1,'webhook',?2,1,0,0 FROM ids",
                params![condition, destination],
            )
            .expect("bulk threshold alerts");
        assert!(reserve_evaluation(&mut too_many_alerts, 60_000_000).is_err());

        let (_directory, mut missing_runtime) = database();
        create(&mut missing_runtime, 1, &configuration(), 0).expect("runtime alert");
        missing_runtime
            .execute_batch("PRAGMA foreign_keys=OFF; DROP TABLE runtime_state")
            .expect("drop runtime state");
        assert!(reserve_evaluation(&mut missing_runtime, 60_000_000).is_err());

        let (_directory, mut changed_reservation) = database();
        create(&mut changed_reservation, 1, &configuration(), 0).expect("reservation alert");
        changed_reservation
            .execute_batch(
                "CREATE TRIGGER ignore_alert_reservation
                 BEFORE UPDATE OF pending_evaluation_end_us ON alerts
                 BEGIN SELECT RAISE(IGNORE); END;",
            )
            .expect("reservation race trigger");
        assert!(reserve_evaluation(&mut changed_reservation, 60_000_000).is_err());

        let (_directory, mut missing_scope) = database();
        create(&mut missing_scope, 1, &configuration(), 0).expect("scoped alert");
        missing_scope
            .execute_batch("PRAGMA foreign_keys=OFF; DROP TABLE projects")
            .expect("drop scope projects");
        assert!(reserve_evaluation(&mut missing_scope, 60_000_000).is_err());

        let (_directory, mut too_many_projects) = database();
        let mut global = configuration();
        global.project_id = None;
        create(&mut too_many_projects, 1, &global, 0).expect("global alert");
        too_many_projects
            .execute(
                "WITH RECURSIVE ids(value) AS (SELECT 2 UNION ALL SELECT value+1 FROM ids WHERE value<1001)
                 INSERT INTO projects(id,slug,name,is_active,created_at_us,updated_at_us)
                 SELECT value,printf('p%d',value),'Project',1,0,0 FROM ids",
                [],
            )
            .expect("bulk active projects");
        assert!(reserve_evaluation(&mut too_many_projects, 60_000_000).is_err());

        let (_directory, mut changed_completion) = database();
        create(&mut changed_completion, 1, &configuration(), 0).expect("completion alert");
        let pending = reserve_evaluation(&mut changed_completion, 60_000_000)
            .expect("reserve completion")
            .expect("pending completion");
        changed_completion
            .execute_batch(
                "CREATE TRIGGER ignore_alert_completion
                 BEFORE UPDATE OF last_evaluated_at_us ON alerts
                 BEGIN SELECT RAISE(IGNORE); END;",
            )
            .expect("completion race trigger");
        assert!(finish_evaluation(&mut changed_completion, &pending, 0, 60_000_000).is_err());
    }
}
