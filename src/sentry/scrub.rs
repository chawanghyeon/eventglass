use std::collections::HashSet;

use serde_json::Value;

const FILTERED: &str = "[Filtered]";
const KNOWN_KEYS: &[&str] = &[
    "authorization",
    "proxyauthorization",
    "cookie",
    "setcookie",
    "password",
    "passwd",
    "accesstoken",
    "refreshtoken",
    "apikey",
    "secret",
    "clientsecret",
];

pub(super) fn scrub(value: &mut Value, custom_keys: &[String]) {
    let mut sensitive: HashSet<String> = KNOWN_KEYS.iter().map(|key| (*key).to_owned()).collect();
    sensitive.extend(
        custom_keys
            .iter()
            .map(|key| normalize_key(key))
            .filter(|key| !key.is_empty()),
    );
    scrub_inner(value, "", &sensitive);
}

fn scrub_inner(value: &mut Value, parent_key: &str, sensitive: &HashSet<String>) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let normalized = normalize_key(key);
                if sensitive.contains(&normalized) {
                    if let Some(wrapper) = child
                        .as_object_mut()
                        .filter(|value| value.contains_key("value") && value.contains_key("type"))
                    {
                        wrapper.insert("value".to_owned(), Value::String(FILTERED.to_owned()));
                        if wrapper.contains_key("type") {
                            wrapper.insert("type".to_owned(), Value::String("string".to_owned()));
                        }
                    } else {
                        *child = Value::String(FILTERED.to_owned());
                    }
                } else if is_url_key(key)
                    && let Some(wrapper) = child.as_object_mut()
                    && let Some(Value::String(text)) = wrapper.get_mut("value")
                {
                    *text = scrub_url(text, sensitive);
                } else {
                    scrub_inner(child, key, sensitive);
                }
            }
        }
        Value::Array(values) => {
            for child in values.iter_mut() {
                if let Value::Array(pair) = child
                    && pair.len() == 2
                    && pair[0]
                        .as_str()
                        .is_some_and(|key| sensitive.contains(&normalize_key(key)))
                {
                    pair[1] = Value::String(FILTERED.to_owned());
                    continue;
                }
                scrub_inner(child, parent_key, sensitive);
            }
        }
        Value::String(text) if is_url_key(parent_key) => {
            *text = scrub_url(text, sensitive);
        }
        _ => {}
    }
}

fn normalize_key(key: &str) -> String {
    key.chars()
        .filter(|character| character.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn is_url_key(key: &str) -> bool {
    let key = normalize_key(key);
    key == "url" || key.ends_with("url")
}

fn scrub_url(input: &str, sensitive: &HashSet<String>) -> String {
    let Some(question) = input.find('?') else {
        return input.to_owned();
    };
    let (prefix, tail) = input.split_at(question + 1);
    let (query, fragment) = tail
        .split_once('#')
        .map_or((tail, ""), |(query, fragment)| (query, fragment));
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if sensitive.contains(&normalize_key(&key)) {
            serializer.append_pair(&key, FILTERED);
        } else {
            serializer.append_pair(&key, &value);
        }
    }
    let query = serializer.finish();
    if fragment.is_empty() {
        format!("{prefix}{query}")
    } else {
        format!("{prefix}{query}#{fragment}")
    }
}
