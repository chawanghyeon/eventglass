//! Bound JSON allocation while decoding, before an oversized DOM can be created.

use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};

use super::SentryError;

const MAX_NODES: usize = 20_000;
const MAX_DEPTH: usize = 64;

pub(super) fn parse(bytes: &[u8]) -> Result<Value, SentryError> {
    parse_with_limit(bytes, MAX_NODES)
}

pub(super) fn parse_with_limit(bytes: &[u8], max_nodes: usize) -> Result<Value, SentryError> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let mut nodes = 0;
    let mut limit_error = None;
    let mut seed = BoundedValue::new(&mut nodes, &mut limit_error);
    seed.max_nodes = max_nodes;
    let result = seed.deserialize(&mut decoder);
    if let Some(error) = limit_error {
        return Err(error);
    }
    let value = result.map_err(|_| SentryError::Malformed("invalid record JSON"))?;
    decoder
        .end()
        .map_err(|_| SentryError::Malformed("trailing record JSON"))?;
    Ok(value)
}

pub(super) struct BoundedValue<'a> {
    nodes: &'a mut usize,
    max_nodes: usize,
    depth: usize,
    limit_error: &'a mut Option<SentryError>,
}

impl<'a> BoundedValue<'a> {
    pub(super) fn new(nodes: &'a mut usize, limit_error: &'a mut Option<SentryError>) -> Self {
        Self {
            nodes,
            max_nodes: MAX_NODES,
            depth: 1,
            limit_error,
        }
    }

    fn child(&mut self) -> BoundedValue<'_> {
        BoundedValue {
            nodes: self.nodes,
            max_nodes: self.max_nodes,
            depth: self.depth + 1,
            limit_error: self.limit_error,
        }
    }
}

impl<'de> DeserializeSeed<'de> for BoundedValue<'_> {
    type Value = Value;

    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<Value, D::Error> {
        *self.nodes += 1;
        if *self.nodes > self.max_nodes || self.depth > MAX_DEPTH {
            *self.limit_error = Some(SentryError::TooLarge("record JSON shape exceeds limit"));
            return Err(de::Error::custom("record JSON shape exceeds limit"));
        }
        decoder.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for BoundedValue<'_> {
    type Value = Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("bounded record JSON")
    }
    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }
    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }
    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| de::Error::custom("nonfinite JSON number"))
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }
    fn visit_string<E: de::Error>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }
    fn visit_seq<S: SeqAccess<'de>>(mut self, mut sequence: S) -> Result<Value, S::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(self.child())? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<M: MapAccess<'de>>(mut self, mut map: M) -> Result<Value, M::Error> {
        let mut values = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            values.insert(key, map.next_value_seed(self.child())?);
        }
        Ok(Value::Object(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shape_limit_stops_decoding_before_reading_an_oversized_tail() {
        // Invalid tail would yield Malformed if the whole payload were decoded first.
        let bytes = format!("[{}invalid-tail]", "0,".repeat(MAX_NODES));
        assert!(matches!(
            parse(bytes.as_bytes()),
            Err(SentryError::TooLarge(_))
        ));
        let deep = format!(
            "{}invalid-tail{}",
            "[".repeat(MAX_DEPTH + 1),
            "]".repeat(MAX_DEPTH + 1)
        );
        assert!(matches!(
            parse(deep.as_bytes()),
            Err(SentryError::TooLarge(_))
        ));
    }

    #[test]
    fn bounded_decoder_preserves_json_types_and_checks_trailing_input() {
        let bytes = br#"{"s":"a\nb","n":null,"b":true,"a":[-1,18446744073709551615,1.5]}"#;
        assert_eq!(
            parse(bytes).unwrap(),
            serde_json::from_slice::<Value>(bytes).unwrap()
        );
        assert!(matches!(parse(b"{} {}"), Err(SentryError::Malformed(_))));
    }
}
