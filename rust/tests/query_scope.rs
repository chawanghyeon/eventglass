use anyhow::Result;
use eventglass::db::{
    self,
    search::{self, ScopeError},
};

#[test]
fn scope_capture_rejects_partial_project_authorization_and_binds_principal() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut db = db::open(&dir.path().join("meta.db"))?;
    db.execute_batch("INSERT INTO runtime_state(singleton,installation_id,storage_generation,next_ingest_seq) VALUES(1,'test','generation',1);
        INSERT INTO users(id,email,password_hash,role,is_active,created_at_us,updated_at_us) VALUES(1,'a@example.test','unused','admin',1,0,0),(2,'b@example.test','unused','member',1,0,0);
        INSERT INTO projects(id,slug,name,is_active,created_at_us,updated_at_us) VALUES(1,'one','One',1,0,0),(2,'two','Two',0,0,0),(3,'three','Three',1,0,0)")?;
    let all = search::capture(&mut db, 1, vec![])?;
    assert_eq!(all.projects, vec![1, 3]);
    let sorted = search::capture(&mut db, 1, vec![3, 1, 1])?;
    assert_eq!(all.hash, sorted.hash);
    let another = search::capture(&mut db, 2, vec![1, 3])?;
    assert_ne!(all.hash, another.hash);
    let denied = search::capture(&mut db, 1, vec![1, 2]).unwrap_err();
    assert_eq!(
        denied.downcast_ref::<ScopeError>(),
        Some(&ScopeError::Forbidden)
    );
    db.execute_batch(
        "UPDATE users SET is_active=0 WHERE id=2; UPDATE runtime_state SET authorization_epoch=1",
    )?;
    assert!(search::capture(&mut db, 2, vec![1]).is_err());
    assert_eq!(search::capture(&mut db, 1, vec![1])?.epoch, 1);
    Ok(())
}

#[test]
fn signing_key_survives_reopen_and_corruption_is_not_replaced() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("meta.db");
    let mut db = db::open(&path)?;
    let original = search::token_key(&mut db)?;
    drop(db);
    let mut reopened = db::open(&path)?;
    assert_eq!(search::token_key(&mut reopened)?, original);
    reopened.execute(
        "UPDATE settings SET value_json='[]' WHERE key='token_hmac_v1'",
        [],
    )?;
    assert!(search::token_key(&mut reopened).is_err());
    assert_eq!(
        reopened.query_row(
            "SELECT value_json FROM settings WHERE key='token_hmac_v1'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        "[]"
    );
    Ok(())
}
