use sha2::{Digest, Sha256};
use uuid::Uuid;

const RECORD_ERROR_DOMAIN: &[u8] = b"eventglass.record.error.v1";
const RECORD_ACCEPTED_DOMAIN: &[u8] = b"eventglass.record.accepted.v1";
const FINGERPRINT_DOMAIN: &[u8] = b"eventglass.fingerprint.v1";
const ISSUE_DOMAIN: &[u8] = b"eventglass.issue.v1";

pub(super) fn error_record_id(project_id: i64, event_id: &str) -> String {
    hash_fields(
        RECORD_ERROR_DOMAIN,
        &[project_id.to_string().as_bytes(), event_id.as_bytes()],
    )
}

pub(super) fn accepted_record_id(
    project_id: i64,
    acceptance_id: Uuid,
    item_ordinal: usize,
    record_ordinal: usize,
) -> String {
    hash_fields(
        RECORD_ACCEPTED_DOMAIN,
        &[
            project_id.to_string().as_bytes(),
            acceptance_id.as_bytes(),
            &(item_ordinal as u64).to_be_bytes(),
            &(record_ordinal as u64).to_be_bytes(),
        ],
    )
}

pub(super) fn fingerprint(parts: &[String]) -> String {
    let canonical = serde_json::to_vec(parts).expect("string arrays always serialize");
    hash_fields(FINGERPRINT_DOMAIN, &[&canonical])
}

pub(super) fn issue_id(project_id: i64, version: u32, fingerprint: &str) -> String {
    hash_fields(
        ISSUE_DOMAIN,
        &[
            project_id.to_string().as_bytes(),
            &version.to_be_bytes(),
            fingerprint.as_bytes(),
        ],
    )
}

fn hash_fields(domain: &[u8], fields: &[&[u8]]) -> String {
    let mut digest = Sha256::new();
    append_field(&mut digest, domain);
    for field in fields {
        append_field(&mut digest, field);
    }
    let bytes = digest.finalize();
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn append_field(digest: &mut Sha256, field: &[u8]) {
    digest.update((field.len() as u64).to_be_bytes());
    digest.update(field);
}
