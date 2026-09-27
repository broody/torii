use async_graphql::Error;
use base64::engine::general_purpose;
use base64::Engine as _;

/// Prefix of the legacy `cursor/{primary}/{secondary}` format, which is ambiguous when a value
/// contains '/'. Still accepted by [`decode`] so cursors issued by older versions keep working.
const LEGACY_CURSOR_PREFIX: &str = "cursor";

/// Encodes a cursor as base64 of the JSON array `[primary, secondary]`, so any character
/// (including '/' and ':') can appear in either value.
pub fn encode(primary: &str, secondary: &str) -> String {
    let cursor = serde_json::to_string(&[primary, secondary])
        .expect("serializing a pair of strings cannot fail");
    general_purpose::STANDARD.encode(cursor.as_bytes())
}

pub fn decode(cursor: &str) -> Result<(String, String), Error> {
    let bytes = general_purpose::STANDARD.decode(cursor)?;
    let cursor = String::from_utf8(bytes)?;

    if cursor.starts_with('[') {
        let [primary, secondary]: [String; 2] =
            serde_json::from_str(&cursor).map_err(|_| Error::new("Invalid cursor format"))?;
        return Ok((primary, secondary));
    }

    decode_legacy(&cursor)
}

fn decode_legacy(cursor: &str) -> Result<(String, String), Error> {
    let parts: Vec<&str> = cursor.split('/').collect();

    if parts.len() != 3 || parts[0] != LEGACY_CURSOR_PREFIX {
        return Err("Invalid cursor format".into());
    }

    Ok((parts[1].to_string(), parts[2].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_values_with_separators() {
        let cases = [
            ("0x1/0x2:0x3", "0x1/0x2:0x3"),
            ("0xabc:0xdef:0x1", "10"),
            ("a/b/c", "d/e"),
            ("", ""),
            ("[\"x\"]", "\"quoted\" \\ back"),
        ];

        for (primary, secondary) in cases {
            let encoded = encode(primary, secondary);
            let (p, s) = decode(&encoded).unwrap();
            assert_eq!(p, primary);
            assert_eq!(s, secondary);
        }
    }

    #[test]
    fn decodes_legacy_format() {
        let legacy = general_purpose::STANDARD.encode("cursor/0x1:0x2/42");
        let (primary, secondary) = decode(&legacy).unwrap();
        assert_eq!(primary, "0x1:0x2");
        assert_eq!(secondary, "42");
    }

    #[test]
    fn rejects_malformed_cursors() {
        // not base64
        assert!(decode("!!!").is_err());
        // legacy format with the wrong number of parts
        assert!(decode(&general_purpose::STANDARD.encode("cursor/a")).is_err());
        assert!(decode(&general_purpose::STANDARD.encode("other/a/b")).is_err());
        // JSON with the wrong shape
        assert!(decode(&general_purpose::STANDARD.encode("[\"a\"]")).is_err());
        assert!(decode(&general_purpose::STANDARD.encode("[\"a\",\"b\",\"c\"]")).is_err());
        assert!(decode(&general_purpose::STANDARD.encode("[1,2]")).is_err());
    }
}
