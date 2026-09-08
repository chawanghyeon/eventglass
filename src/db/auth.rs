//! Atomic authentication and user-management operations, independent of HTTP.

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum AuthDbError {
    Forbidden,
    SetupCompleted,
    SetupUnauthorized,
    Conflict,
    NotFound,
    LastAdmin,
    InvalidRole,
}

impl std::fmt::Display for AuthDbError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for AuthDbError {}

#[derive(Debug, Clone)]
pub struct LoginCandidate {
    pub id: i64,
    pub password_hash: String,
}

#[derive(Debug, Clone)]
pub struct Principal {
    pub id: i64,
    pub email: String,
    pub role: String,
    pub authorization_epoch: i64,
}

#[derive(Debug, Serialize)]
pub struct User {
    pub id: String,
    pub email: String,
    pub role: String,
    pub is_active: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct SetupSetting {
    hash: String,
    expires_at_us: i64,
}

fn require_admin(db: &Connection, actor: i64) -> Result<()> {
    let allowed: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM users WHERE id=?1 AND role='admin' AND is_active=1)",
        [actor],
        |row| row.get(0),
    )?;
    if !allowed {
        return Err(AuthDbError::Forbidden.into());
    }
    Ok(())
}

fn validate_role(role: &str) -> Result<()> {
    if !matches!(role, "admin" | "member") {
        return Err(AuthDbError::InvalidRole.into());
    }
    Ok(())
}

pub fn issue_setup_token(
    db: &mut Connection,
    token_hash: &str,
    expires_at_us: i64,
    now_us: i64,
) -> Result<()> {
    let tx = db.transaction()?;
    let users: i64 = tx.query_row("SELECT count(*) FROM users", [], |row| row.get(0))?;
    if users != 0 {
        return Err(AuthDbError::SetupCompleted.into());
    }
    let setting = serde_json::to_string(&SetupSetting {
        hash: token_hash.to_owned(),
        expires_at_us,
    })?;
    tx.execute(
        "INSERT INTO settings(key,value_json,updated_at_us) VALUES('setup',?1,?2)
         ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at_us=excluded.updated_at_us",
        params![setting, now_us],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn complete_setup(
    db: &mut Connection,
    presented_token_hash: &str,
    email: &str,
    password_hash: &str,
    now_us: i64,
) -> Result<i64> {
    let tx = db.transaction()?;
    let user_count: i64 = tx.query_row("SELECT count(*) FROM users", [], |row| row.get(0))?;
    let setting: Option<String> = tx
        .query_row(
            "SELECT value_json FROM settings WHERE key='setup'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let parsed = setting
        .as_deref()
        .map(serde_json::from_str::<SetupSetting>)
        .transpose()?;
    let authorized = user_count == 0
        && parsed.as_ref().is_some_and(|setting| {
            setting.expires_at_us > now_us
                && crate::auth::secure_eq(&setting.hash, presented_token_hash)
        });
    if !authorized {
        return Err(AuthDbError::SetupUnauthorized.into());
    }
    tx.execute(
        "INSERT INTO users(email,password_hash,role,created_at_us,updated_at_us)
         VALUES(?1,?2,'admin',?3,?3)",
        params![email, password_hash, now_us],
    )?;
    let id = tx.last_insert_rowid();
    tx.execute("DELETE FROM settings WHERE key='setup'", [])?;
    tx.execute(
        "UPDATE runtime_state SET authorization_epoch=authorization_epoch+1",
        [],
    )?;
    tx.commit()?;
    Ok(id)
}

pub fn login_candidate(db: &Connection, email: &str) -> Result<Option<LoginCandidate>> {
    Ok(db
        .query_row(
            "SELECT id,password_hash FROM users WHERE email=?1 AND is_active=1",
            [email],
            |row| {
                Ok(LoginCandidate {
                    id: row.get(0)?,
                    password_hash: row.get(1)?,
                })
            },
        )
        .optional()?)
}

pub fn create_session(
    db: &mut Connection,
    user_id: i64,
    expected_password_hash: &str,
    token_hash: &str,
    now_us: i64,
    expires_at_us: i64,
) -> Result<bool> {
    let tx = db.transaction()?;
    let inserted = tx.execute(
        "INSERT INTO sessions(token_hash,user_id,expires_at_us,created_at_us,last_seen_at_us)
         SELECT ?1,id,?2,?3,?3 FROM users
         WHERE id=?4 AND password_hash=?5 AND is_active=1",
        params![
            token_hash,
            expires_at_us,
            now_us,
            user_id,
            expected_password_hash
        ],
    )? == 1;
    tx.commit()?;
    Ok(inserted)
}

pub fn authenticate(db: &Connection, token_hash: &str, now_us: i64) -> Result<Option<Principal>> {
    Ok(db
        .query_row(
            "SELECT u.id,u.email,u.role,r.authorization_epoch
             FROM sessions s
             JOIN users u ON u.id=s.user_id
             JOIN runtime_state r ON r.singleton=1
             WHERE s.token_hash=?1 AND s.expires_at_us>?2 AND u.is_active=1",
            params![token_hash, now_us],
            |row| {
                Ok(Principal {
                    id: row.get(0)?,
                    email: row.get(1)?,
                    role: row.get(2)?,
                    authorization_epoch: row.get(3)?,
                })
            },
        )
        .optional()?)
}

pub fn logout(db: &Connection, token_hash: &str) -> Result<()> {
    db.execute("DELETE FROM sessions WHERE token_hash=?1", [token_hash])?;
    Ok(())
}

pub fn list_users(db: &Connection, actor: i64) -> Result<Vec<User>> {
    require_admin(db, actor)?;
    let mut statement = db.prepare("SELECT id,email,role,is_active FROM users ORDER BY id")?;
    Ok(statement
        .query_map([], |row| {
            Ok(User {
                id: row.get::<_, i64>(0)?.to_string(),
                email: row.get(1)?,
                role: row.get(2)?,
                is_active: row.get(3)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

pub fn create_user(
    db: &mut Connection,
    actor: i64,
    email: &str,
    password_hash: &str,
    role: &str,
    now_us: i64,
) -> Result<i64> {
    validate_role(role)?;
    let tx = db.transaction()?;
    require_admin(&tx, actor)?;
    let exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM users WHERE email=?1)",
        [email],
        |row| row.get(0),
    )?;
    if exists {
        return Err(AuthDbError::Conflict.into());
    }
    tx.execute(
        "INSERT INTO users(email,password_hash,role,created_at_us,updated_at_us)
         VALUES(?1,?2,?3,?4,?4)",
        params![email, password_hash, role, now_us],
    )?;
    let id = tx.last_insert_rowid();
    tx.execute(
        "UPDATE runtime_state SET authorization_epoch=authorization_epoch+1",
        [],
    )?;
    tx.commit()?;
    Ok(id)
}

pub fn update_user(
    db: &mut Connection,
    actor: i64,
    target: i64,
    role: Option<&str>,
    is_active: Option<bool>,
    now_us: i64,
) -> Result<()> {
    if let Some(role) = role {
        validate_role(role)?;
    }
    let tx = db.transaction()?;
    require_admin(&tx, actor)?;
    let current: Option<(String, bool)> = tx
        .query_row(
            "SELECT role,is_active FROM users WHERE id=?1",
            [target],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (current_role, current_active) = current.ok_or(AuthDbError::NotFound)?;
    let next_role = role.unwrap_or(&current_role);
    let next_active = is_active.unwrap_or(current_active);
    if current_role == next_role && current_active == next_active {
        tx.commit()?;
        return Ok(());
    }
    if current_role == "admin" && current_active && (next_role != "admin" || !next_active) {
        let active_admins: i64 = tx.query_row(
            "SELECT count(*) FROM users WHERE role='admin' AND is_active=1",
            [],
            |row| row.get(0),
        )?;
        if active_admins <= 1 {
            return Err(AuthDbError::LastAdmin.into());
        }
    }
    tx.execute(
        "UPDATE users SET role=?1,is_active=?2,updated_at_us=?3 WHERE id=?4",
        params![next_role, next_active, now_us, target],
    )?;
    tx.execute("DELETE FROM sessions WHERE user_id=?1", [target])?;
    tx.execute(
        "UPDATE runtime_state SET authorization_epoch=authorization_epoch+1",
        [],
    )?;
    tx.commit()?;
    Ok(())
}
