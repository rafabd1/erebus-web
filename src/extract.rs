use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

const MAX_PARAMETER_BODY_BYTES: usize = 512 * 1024;
const MAX_JSON_DEPTH: usize = 12;
const MAX_PARAMETERS: usize = 2_048;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BodyParameter {
    pub name: String,
    pub path: String,
    pub value_type: &'static str,
}

pub fn body_parameters(content_type: Option<&str>, bytes: Option<&[u8]>) -> Vec<BodyParameter> {
    let Some(bytes) = bytes.filter(|bytes| bytes.len() <= MAX_PARAMETER_BODY_BYTES) else {
        return Vec::new();
    };
    let content_type = content_type
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match content_type.as_str() {
        "application/json" | "application/ld+json" | "application/problem+json" => {
            json_parameters(bytes)
        }
        "application/x-www-form-urlencoded" => form_parameters(bytes),
        _ if content_type.ends_with("+json") => json_parameters(bytes),
        _ => Vec::new(),
    }
}

fn json_parameters(bytes: &[u8]) -> Vec<BodyParameter> {
    let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
        return Vec::new();
    };
    let mut parameters = BTreeMap::<String, BodyParameter>::new();
    visit_json(&value, "", 0, &mut parameters);
    parameters.into_values().collect()
}

fn visit_json(
    value: &Value,
    path: &str,
    depth: usize,
    parameters: &mut BTreeMap<String, BodyParameter>,
) {
    if depth >= MAX_JSON_DEPTH || parameters.len() >= MAX_PARAMETERS {
        return;
    }
    match value {
        Value::Object(fields) => {
            for (name, value) in fields {
                if parameters.len() >= MAX_PARAMETERS {
                    break;
                }
                let field_path = if path.is_empty() {
                    name.clone()
                } else {
                    format!("{path}.{name}")
                };
                parameters
                    .entry(field_path.clone())
                    .or_insert_with(|| BodyParameter {
                        name: name.clone(),
                        path: field_path.clone(),
                        value_type: value_type(value),
                    });
                visit_json(value, &field_path, depth + 1, parameters);
            }
        }
        Value::Array(items) => {
            let array_path = format!("{path}[]");
            for item in items.iter().take(32) {
                visit_json(item, &array_path, depth + 1, parameters);
                if parameters.len() >= MAX_PARAMETERS {
                    break;
                }
            }
        }
        _ => {}
    }
}

fn form_parameters(bytes: &[u8]) -> Vec<BodyParameter> {
    let mut parameters = BTreeMap::<String, BodyParameter>::new();
    for (name, _) in url::form_urlencoded::parse(bytes) {
        if parameters.len() >= MAX_PARAMETERS {
            break;
        }
        let name = name.into_owned();
        parameters
            .entry(name.clone())
            .or_insert_with(|| BodyParameter {
                path: name.clone(),
                name,
                value_type: "string",
            });
    }
    parameters.into_values().collect()
}

fn value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_nested_json_shape_without_values() {
        let parameters = body_parameters(
            Some("application/json; charset=utf-8"),
            Some(br#"{"user":{"email":"a@example.com"},"roles":[{"id":1},{"id":2}]}"#),
        );
        assert!(
            parameters
                .iter()
                .any(|parameter| parameter.path == "user.email")
        );
        assert!(
            parameters
                .iter()
                .any(|parameter| parameter.path == "roles[].id")
        );
        assert!(!format!("{parameters:?}").contains("a@example.com"));
    }

    #[test]
    fn deduplicates_form_keys() {
        let parameters = body_parameters(
            Some("application/x-www-form-urlencoded"),
            Some(b"id=1&id=2&name=test"),
        );
        assert_eq!(
            parameters
                .iter()
                .map(|parameter| parameter.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "name"]
        );
    }

    #[test]
    fn ignores_oversized_and_unknown_bodies() {
        assert!(body_parameters(Some("text/plain"), Some(b"a=1")).is_empty());
        assert!(
            body_parameters(
                Some("application/json"),
                Some(&vec![b'a'; MAX_PARAMETER_BODY_BYTES + 1])
            )
            .is_empty()
        );
    }
}
