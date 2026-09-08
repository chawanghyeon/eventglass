use eventglass::search::tokens::{Position, TokenCodec, TokenContext, TokenError, TokenKind};

fn context() -> TokenContext {
    TokenContext {
        storage_generation: "generation-a".into(),
        authorization_epoch: 1,
        authorization_hash: "authorized-project-set".into(),
        request_hash: "canonical-query-scope".into(),
    }
}

#[test]
fn token_domains_tampering_expiry_and_scope_are_enforced() {
    let codec = TokenCodec::new([42; 32]);
    let context = context();
    let token = codec
        .issue(context.clone(), 100, Position::Read, 1_000_000)
        .unwrap();
    assert_eq!(
        codec
            .verify(&token, TokenKind::Read, &context, 1_000_001)
            .unwrap()
            .watermark,
        100
    );
    assert_eq!(
        codec.verify(&token, TokenKind::Rows, &context, 1_000_001),
        Err(TokenError::Invalid)
    );
    assert_eq!(
        TokenCodec::new([43; 32]).verify(&token, TokenKind::Read, &context, 1_000_001),
        Err(TokenError::Invalid)
    );
    let mut altered = token.clone().into_bytes();
    altered[5] = if altered[5] == b'A' { b'B' } else { b'A' };
    assert_eq!(
        codec.verify(
            std::str::from_utf8(&altered).unwrap(),
            TokenKind::Read,
            &context,
            1_000_001
        ),
        Err(TokenError::Invalid)
    );
    assert_eq!(
        codec.verify(&token, TokenKind::Read, &context, 901_000_000),
        Err(TokenError::Expired)
    );
    let mut changed = context.clone();
    changed.authorization_epoch += 1;
    assert_eq!(
        codec.verify(&token, TokenKind::Read, &changed, 1_000_001),
        Err(TokenError::AuthorizationChanged)
    );
    changed = context.clone();
    changed.storage_generation = "restored-generation".into();
    assert_eq!(
        codec.verify(&token, TokenKind::Read, &changed, 1_000_001),
        Err(TokenError::GenerationChanged)
    );
    changed = context.clone();
    changed.request_hash = "another-filter".into();
    assert_eq!(
        codec.verify(&token, TokenKind::Read, &changed, 1_000_001),
        Err(TokenError::Invalid)
    );
}

#[test]
fn opaque_tokens_preserve_large_integer_identity_and_kind_ttls() {
    let codec = TokenCodec::new([7; 32]);
    let seq = 9_007_199_254_740_993;
    let row = Position::Rows {
        timestamp_us: 1788860000000000,
        ingest_seq: seq,
        record_id: "a".repeat(64),
    };
    let token = codec.issue(context(), seq, row.clone(), 1).unwrap();
    assert_eq!(
        codec
            .verify(&token, TokenKind::Rows, &context(), 2)
            .unwrap()
            .position,
        row
    );
    let detail = Position::Detail {
        project_id: seq,
        shard_id: uuid::Uuid::new_v4().to_string(),
        record_id: "b".repeat(64),
    };
    let token = codec.issue(context(), seq, detail.clone(), 1).unwrap();
    assert_eq!(
        codec
            .verify(&token, TokenKind::Detail, &context(), 900_000_001)
            .unwrap()
            .position,
        detail
    );
    assert_eq!(
        codec.verify(&token, TokenKind::Detail, &context(), 3_600_000_001),
        Err(TokenError::Expired)
    );
    assert!(
        codec
            .issue(context(), 10, Position::Live { scan_seq: 11 }, 1)
            .is_err()
    );
    assert_eq!(
        codec.verify(&"A".repeat(13000), TokenKind::Read, &context(), 1),
        Err(TokenError::Invalid)
    );
}
