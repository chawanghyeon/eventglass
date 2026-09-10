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

#[derive(Debug, Serialize)]
pub struct ProjectKey {
    pub id: String,
    pub public_key: String,
}

pub fn list_keys(db: &Connection, actor: i64, project: i64) -> Result<Vec<ProjectKey>> {
    require_admin(db, actor)?;
    let exists: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE id=?1)",
        [project],
        |row| row.get(0),
    )?;
    if !exists {
        return Err(ManagementError::NotFound.into());
    }
    let mut query = db.prepare("SELECT id, public_key FROM project_keys WHERE project_id=?1 AND revoked_at_us IS NULL ORDER BY id DESC")?;
    Ok(query
        .query_map([project], |row| {
            Ok(ProjectKey {
                id: row.get::<_, i64>(0)?.to_string(),
                public_key: row.get(1)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database() -> (tempfile::TempDir, Connection) {
        let directory = tempfile::tempdir().unwrap();
        let db = crate::db::open(&directory.path().join("meta.db")).unwrap();
        db.execute_batch(
            "INSERT INTO users(id,email,password_hash,role,is_active,created_at_us,updated_at_us)
             VALUES(1,'admin@example.test','x','admin',1,0,0),
                   (2,'member@example.test','x','member',1,0,0);
             INSERT INTO projects(id,slug,name,is_active,created_at_us,updated_at_us)
             VALUES(1,'active','Active',1,0,0),(2,'inactive','Inactive',0,0,0)",
        )
        .unwrap();
        (directory, db)
    }

    fn kind(error: anyhow::Error) -> String {
        error.downcast_ref::<ManagementError>().unwrap().to_string()
    }

    #[test]
    fn project_management_enforces_admin_conflict_and_missing_boundaries() -> Result<()> {
        let (_directory, mut db) = database();
        assert_eq!(
            kind(create(&mut db, 2, "new", "New").unwrap_err()),
            "Forbidden"
        );
        assert_eq!(
            kind(create(&mut db, 1, "active", "Again").unwrap_err()),
            "Conflict"
        );
        let id = create(&mut db, 1, "new", "New")?;
        assert_eq!(list(&db, 2)?.len(), 2);
        assert_eq!(list(&db, 1)?.len(), 3);
        assert_eq!(
            kind(set_active(&mut db, 1, 999, false).unwrap_err()),
            "NotFound"
        );
        assert_eq!(
            kind(create_key(&mut db, 1, 999, "key").unwrap_err()),
            "NotFound"
        );
        let key = create_key(&mut db, 1, id, "key")?;
        assert_eq!(list_keys(&db, 1, id)?.len(), 1);
        revoke_key(&mut db, 1, id, key)?;
        assert!(list_keys(&db, 1, id)?.is_empty());
        assert_eq!(
            kind(revoke_key(&mut db, 1, id, 999).unwrap_err()),
            "NotFound"
        );
        assert_eq!(kind(list_keys(&db, 1, 999).unwrap_err()), "NotFound");
        assert!(std::error::Error::source(&ManagementError::Forbidden).is_none());
        Ok(())
    }

    fn admin_only_database() -> Connection {
        let database = Connection::open_in_memory().expect("open project failure database");
        database
            .execute_batch(
                "CREATE TABLE users(id INTEGER,role TEXT,is_active INTEGER);
                 INSERT INTO users VALUES(1,'admin',1);",
            )
            .expect("seed minimum admin schema");
        database
    }

    #[test]
    fn sqlite_schema_failures_are_never_treated_as_authorization_or_empty_results() {
        let mut empty = Connection::open_in_memory().expect("open empty project database");
        assert!(create(&mut empty, 1, "new", "New").is_err());
        assert!(list(&empty, 1).is_err());

        let mut without_projects = admin_only_database();
        assert!(list(&without_projects, 1).is_err());
        assert!(create(&mut without_projects, 1, "new", "New").is_err());
        assert!(set_active(&mut without_projects, 1, 1, false).is_err());
        assert!(list_keys(&without_projects, 1, 1).is_err());

        let mut incomplete_projects = admin_only_database();
        incomplete_projects
            .execute_batch("CREATE TABLE projects(slug TEXT);")
            .expect("create incomplete project schema");
        assert!(create(&mut incomplete_projects, 1, "new", "New").is_err());
    }
}
