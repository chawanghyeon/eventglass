use serde_json::Value;

use super::SentryError;

const MAX_HEADER_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub(super) struct Envelope<'a> {
    pub header: Value,
    pub items: Vec<EnvelopeItem<'a>>,
}

#[derive(Debug)]
pub(super) struct EnvelopeItem<'a> {
    pub ordinal: usize,
    pub kind: String,
    pub payload: &'a [u8],
}

pub(super) fn parse(input: &[u8], max_items: usize) -> Result<Envelope<'_>, SentryError> {
    let (header, mut cursor) = parse_header(input)?;
    let mut items = Vec::new();

    while cursor < input.len() {
        if items.len() >= max_items {
            return Err(SentryError::TooLarge("envelope item count exceeds limit"));
        }
        let (item_header_bytes, payload_start) =
            line(input, cursor, "missing item header newline")?;
        if item_header_bytes.len() > MAX_HEADER_BYTES {
            return Err(SentryError::TooLarge("item header exceeds limit"));
        }
        let item_header = json_object(item_header_bytes, "invalid item header")?;
        let kind = item_header
            .get("type")
            .and_then(Value::as_str)
            .ok_or(SentryError::Malformed("item type must be a string"))?
            .to_owned();

        let (payload, next) = match item_header.get("length") {
            Some(length) => {
                let length = length
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or(SentryError::Malformed(
                        "item length must be a nonnegative integer",
                    ))?;
                let payload_end = payload_start
                    .checked_add(length)
                    .ok_or(SentryError::TooLarge("item length overflows address space"))?;
                if payload_end > input.len() {
                    return Err(SentryError::Malformed(
                        "item payload is shorter than declared length",
                    ));
                }
                if payload_end == input.len() {
                    (&input[payload_start..payload_end], payload_end)
                } else if input[payload_end] == b'\n' {
                    (&input[payload_start..payload_end], payload_end + 1)
                } else {
                    return Err(SentryError::Malformed(
                        "length-delimited item is missing its separator",
                    ));
                }
            }
            None => {
                let payload_end = input[payload_start..]
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(input.len(), |offset| payload_start + offset);
                let next = if payload_end < input.len() {
                    payload_end + 1
                } else {
                    payload_end
                };
                (&input[payload_start..payload_end], next)
            }
        };

        items.push(EnvelopeItem {
            ordinal: items.len(),
            kind,
            payload,
        });
        cursor = next;
    }

    Ok(Envelope { header, items })
}

pub(super) fn parse_header(input: &[u8]) -> Result<(Value, usize), SentryError> {
    let (header_bytes, cursor) = line(input, 0, "missing envelope header newline")?;
    if header_bytes.len() > MAX_HEADER_BYTES {
        return Err(SentryError::TooLarge("envelope header exceeds limit"));
    }
    Ok((
        json_object(header_bytes, "invalid envelope header")?,
        cursor,
    ))
}

fn line<'a>(
    input: &'a [u8],
    start: usize,
    missing: &'static str,
) -> Result<(&'a [u8], usize), SentryError> {
    let newline = input[start..]
        .iter()
        .position(|byte| *byte == b'\n')
        .map(|offset| start + offset)
        .ok_or(SentryError::Malformed(missing))?;
    Ok((&input[start..newline], newline + 1))
}

fn json_object(input: &[u8], invalid: &'static str) -> Result<Value, SentryError> {
    let value = super::json::parse(input)?;
    if !value.is_object() {
        return Err(SentryError::Malformed(invalid));
    }
    Ok(value)
}
