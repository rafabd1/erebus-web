use blake3::Hasher;

use crate::model::HeaderField;

const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

pub fn request_fingerprint(
    method: &str,
    url: &str,
    headers: &[HeaderField],
    body_hash: Option<&str>,
    body_state: &str,
    body_original_size: u64,
) -> String {
    let mut hasher = Hasher::new();
    update_field(&mut hasher, method.trim().to_ascii_uppercase().as_bytes());
    update_field(&mut hasher, url.as_bytes());
    for (name, value) in canonical_headers(headers) {
        update_field(&mut hasher, name.as_bytes());
        update_field(&mut hasher, value.as_bytes());
    }
    update_field(&mut hasher, body_hash.unwrap_or_default().as_bytes());
    update_field(&mut hasher, body_state.as_bytes());
    update_field(&mut hasher, &body_original_size.to_le_bytes());
    hasher.finalize().to_hex().to_string()
}

pub fn response_fingerprint(
    status: u16,
    headers: &[HeaderField],
    body_hash: Option<&str>,
    body_state: &str,
    body_original_size: u64,
) -> String {
    let mut hasher = Hasher::new();
    update_field(&mut hasher, status.to_string().as_bytes());
    for (name, value) in canonical_headers(headers) {
        update_field(&mut hasher, name.as_bytes());
        update_field(&mut hasher, value.as_bytes());
    }
    update_field(&mut hasher, body_hash.unwrap_or_default().as_bytes());
    update_field(&mut hasher, body_state.as_bytes());
    update_field(&mut hasher, &body_original_size.to_le_bytes());
    hasher.finalize().to_hex().to_string()
}

pub fn content_type(headers: &[HeaderField]) -> Option<String> {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case("content-type"))
        .map(|header| {
            header
                .value
                .split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
        })
        .filter(|value| !value.is_empty())
}

fn canonical_headers(headers: &[HeaderField]) -> Vec<(String, String)> {
    let mut canonical = headers
        .iter()
        .filter_map(|header| {
            let name = header.name.trim().to_ascii_lowercase();
            (!HOP_BY_HOP_HEADERS.contains(&name.as_str()))
                .then(|| (name, header.value.trim().to_owned()))
        })
        .collect::<Vec<_>>();
    canonical.sort();
    canonical
}

fn update_field(hasher: &mut Hasher, field: &[u8]) {
    hasher.update(&(field.len() as u64).to_le_bytes());
    hasher.update(field);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(name: &str, value: &str) -> HeaderField {
        HeaderField {
            name: name.into(),
            value: value.into(),
        }
    }

    #[test]
    fn header_order_and_case_do_not_fragment_exact_variants() {
        let first = [header("Accept", "application/json"), header("X-Test", "1")];
        let second = [header("x-test", "1"), header("accept", "application/json")];
        assert_eq!(
            request_fingerprint("get", "https://example.com/a", &first, None, "complete", 0),
            request_fingerprint("GET", "https://example.com/a", &second, None, "complete", 0)
        );
    }

    #[test]
    fn different_payloads_remain_different_variants() {
        assert_ne!(
            request_fingerprint(
                "POST",
                "https://example.com/a",
                &[],
                Some("one"),
                "complete",
                3
            ),
            request_fingerprint(
                "POST",
                "https://example.com/a",
                &[],
                Some("two"),
                "complete",
                3
            )
        );
    }

    #[test]
    fn missing_and_empty_complete_bodies_are_distinct() {
        assert_ne!(
            request_fingerprint("POST", "https://example.com/a", &[], None, "unavailable", 0),
            request_fingerprint("POST", "https://example.com/a", &[], None, "complete", 0)
        );
    }
}
