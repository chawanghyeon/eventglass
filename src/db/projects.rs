//! Atomic project management operations, independent of HTTP.

use anyhow::Result;
use rusqlite::{Connection, params};
use serde::Serialize;

use crate::model::now_us;

#[derive(Debug, Serialize)]
pub struct Project {
    pub id: String,
    pub slug: String,
    pub name: String,
    pub is_active: bool,
}

#[derive(Debug)]
pub enum ManagementError {
    Forbidden,
    Conflict,
    NotFound,
}

impl std::fmt::Display for ManagementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ManagementError {}

fn require_admin(db: &Connection, actor: i64) -> Result<()> {
    let permitted: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM users WHERE id=?1 AND role='admin' AND is_active=1)",
        [actor],
        |r| r.get(0),
    )?;
    if !permitted {
        return Err(ManagementError::Forbidden.into());
    }
    Ok(())
}

pub fn list(db: &Connection, actor: i64) -> Result<Vec<Project>> {
    let role: String = db.query_row(
        "SELECT role FROM users WHERE id=?1 AND is_active=1",
        [actor],
        |r| r.get(0),
    )?;
    let mut statement = db.prepare(
        "SELECT id,slug,name,is_active FROM projects WHERE is_active=1 OR ?1='admin' ORDER BY id",
    )?;
    Ok(statement
        .query_map([role], |r| {
            Ok(Project {
                id: r.get::<_, i64>(0)?.to_string(),
                slug: r.get(1)?,
                name: r.get(2)?,
                is_active: r.get(3)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?)
}

pub fn create(db: &mut Connection, actor: i64, slug: &str, name: &str) -> Result<i64> {
    let tx = db.transaction()?;
    require_admin(&tx, actor)?;
    let exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE slug=?1)",
        [slug],
        |r| r.get(0),
    )?;
    if exists {
        return Err(ManagementError::Conflict.into());
    }
    tx.execute(
        "INSERT INTO projects(slug,name,created_at_us,updated_at_us) VALUES(?1,?2,?3,?3)",
        params![slug, name, now_us()?],
    )?;
    let id = tx.last_insert_rowid();
    tx.commit()?;
    Ok(id)
}

pub fn set_active(db: &mut Connection, actor: i64, id: i64, active: bool) -> Result<()> {
    let tx = db.transaction()?;
    require_admin(&tx, actor)?;
    if tx.execute(
        "UPDATE projects SET is_active=?1,updated_at_us=?2 WHERE id=?3",
        params![active, now_us()?, id],
    )? == 0
    {
        return Err(ManagementError::NotFound.into());
    }
    tx.execute(
        "UPDATE runtime_state SET authorization_epoch=authorization_epoch+1",
        [],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn create_key(db: &mut Connection, actor: i64, project: i64, key: &str) -> Result<i64> {
    let tx = db.transaction()?;
    require_admin(&tx, actor)?;
    if tx.execute("INSERT INTO project_keys(project_id,public_key,created_at_us) SELECT id,?1,?2 FROM projects WHERE id=?3 AND is_active=1",params![key,now_us()?,project])?==0 {return Err(ManagementError::NotFound.into());}
    let id = tx.last_insert_rowid();
    tx.commit()?;
    Ok(id)
}

pub fn revoke_key(db: &mut Connection, actor: i64, project: i64, key: i64) -> Result<()> {
    let tx = db.transaction()?;
    require_admin(&tx, actor)?;
    if tx.execute("UPDATE project_keys SET revoked_at_us=coalesce(revoked_at_us,?1) WHERE id=?2 AND project_id=?3",params![now_us()?,key,project])?==0 {return Err(ManagementError::NotFound.into());}
    tx.commit()?;
    Ok(())
}
