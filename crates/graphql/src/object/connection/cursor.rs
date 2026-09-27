use async_graphql::Error;
use base64::engine::general_purpose;
use base64::Engine as _;
use sqlx::sqlite::SqliteRow;
use sqlx::{Column, Row};

/// Prefix of the legacy `cursor/{primary}/{secondary}` format, which is ambiguous when a value
/// contains '/'. Still accepted by [`decode`] so cursors issued by older versions keep working.
const LEGACY_CURSOR_PREFIX: &str = "cursor";

/// Encodes a cursor as base64 of the JSON array `[primary, secondary]`, so any character
/// (including '/' and ':') can appear in either value.
pub fn encode(primary: &str, secondary: &str) -> String {
    encode_nullable(primary, Some(secondary))
}

/// Like [`encode`], but a `None` secondary value (a NULL order column) is encoded as JSON `null`.
fn encode_nullable(primary: &str, secondary: Option<&str>) -> String {
    let cursor = serde_json::to_string(&(primary, secondary))
        .expect("serializing a pair of strings cannot fail");
    general_purpose::STANDARD.encode(cursor.as_bytes())
}

/// Encodes the cursor of `row` from its `id_column` and the `order_column` the listing is ordered
/// by. Column names are matched case-insensitively, as SQLite does, so an order field lowercased
/// from a GraphQL enum item still finds a camelCase column.
pub fn encode_row(row: &SqliteRow, id_column: &str, order_column: &str) -> sqlx::Result<String> {
    let primary = column_text(row, id_column)?
        .ok_or_else(|| sqlx::Error::Decode(format!("NULL cursor column {id_column}").into()))?;
    // The order column can be NULL, e.g. for a member added by a model upgrade.
    let secondary = column_text(row, order_column)?;
    Ok(encode_nullable(&primary, secondary.as_deref()))
}

fn column_text(row: &SqliteRow, column: &str) -> sqlx::Result<Option<String>> {
    let columns = row.columns();
    let index = columns
        .iter()
        .position(|c| c.name() == column)
        .or_else(|| {
            columns
                .iter()
                .position(|c| c.name().eq_ignore_ascii_case(column))
        })
        .ok_or_else(|| sqlx::Error::ColumnNotFound(column.to_string()))?;

    // INTEGER values are read back as their decimal text representation.
    row.try_get_unchecked::<Option<String>, _>(index)
}

/// Decodes a cursor into its primary and secondary values. A NULL secondary value decodes as an
/// empty string; use [`decode_nullable`] to tell them apart.
pub fn decode(cursor: &str) -> Result<(String, String), Error> {
    decode_nullable(cursor).map(|(primary, secondary)| (primary, secondary.unwrap_or_default()))
}

/// Decodes a cursor into its primary value and its secondary value, `None` when the order column
/// of the cursor row was NULL.
pub fn decode_nullable(cursor: &str) -> Result<(String, Option<String>), Error> {
    let bytes = general_purpose::STANDARD.decode(cursor)?;
    let cursor = String::from_utf8(bytes)?;

    if cursor.starts_with('[') {
        let (primary, secondary): (String, Option<String>) =
            serde_json::from_str(&cursor).map_err(|_| Error::new("Invalid cursor format"))?;
        return Ok((primary, secondary));
    }

    decode_legacy(&cursor).map(|(primary, secondary)| (primary, Some(secondary)))
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
    fn round_trips_null_secondary_values() {
        let encoded = encode_nullable("0x1", None);
        assert_eq!(
            decode_nullable(&encoded).unwrap(),
            ("0x1".to_string(), None)
        );
        assert_eq!(
            decode(&encoded).unwrap(),
            ("0x1".to_string(), String::new())
        );

        let encoded = encode("0x1", "");
        assert_eq!(
            decode_nullable(&encoded).unwrap(),
            ("0x1".to_string(), Some(String::new()))
        );
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
