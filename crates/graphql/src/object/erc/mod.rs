use async_graphql::Value;
use tracing::debug;

use self::erc_token::{Erc1155Token, Erc20Token, Erc721Token, ErcTokenType};
use super::connection::cursor;
use crate::query::order::CursorDirection;

pub mod erc_token;
pub mod token_balance;
pub mod token_transfer;

/// Builds the keyset condition for a pagination cursor, returning the SQL condition and the value
/// to bind to its placeholder.
///
/// `id_column` must be qualified with its table alias: the ERC queries join tables that also have
/// an `id` column.
fn handle_cursor(
    cursor: &str,
    direction: CursorDirection,
    id_column: &str,
) -> sqlx::Result<(String, String)> {
    match cursor::decode(cursor) {
        Ok((id, _)) => Ok((format!("{} {} ?", id_column, direction.as_ref()), id)),
        Err(_) => Err(sqlx::Error::Decode("Invalid cursor format".into())),
    }
}

/// SQL condition keeping only the rows [`map_token`] can represent: ERC20 tokens and individual
/// ERC721/ERC1155 tokens. Contract-level rows of NFT collections and other contract types are
/// excluded, so that count and page queries agree and no row is dropped after fetching.
///
/// Expects the `contracts` table to be joined as `c`; `token_key_column` holds the `tokens.id` of
/// the row's token.
fn mappable_token_condition(token_key_column: &str) -> String {
    format!(
        "(UPPER(c.contract_type) = 'ERC20' OR (UPPER(c.contract_type) IN ('ERC721', 'ERC1155') \
         AND instr({token_key_column}, ':') > 0))"
    )
}

#[derive(Debug, Clone)]
pub struct ConnectionEdge<T> {
    pub node: T,
    pub cursor: String,
}

#[derive(Debug, Clone)]
pub struct Connection<T> {
    pub total_count: i64,
    pub edges: Vec<ConnectionEdge<T>>,
    pub page_info: Value,
}

/// Columns of a `tokens` row (joined with `contracts`) needed to build an [`ErcTokenType`].
#[derive(Debug)]
pub(crate) struct TokenColumns<'a> {
    pub contract_type: &'a str,
    /// Key of the token in the `tokens` table: the contract address for ERC20 tokens and
    /// `contract_address:token_id` for individual ERC721/ERC1155 tokens. ERC721/ERC1155
    /// collections also have a contract-level row keyed by the bare contract address.
    pub token_key: &'a str,
    pub contract_address: &'a str,
    pub name: &'a str,
    pub symbol: &'a str,
    pub decimals: u8,
    pub metadata: &'a str,
    /// Balance or transferred amount. Ignored for ERC721 tokens.
    pub amount: &'a str,
}

#[derive(Debug, thiserror::Error)]
pub enum TokenMappingError {
    #[error(
        "`{token_key}` is the contract-level entry of an {contract_type} collection, not an \
         individual token (expected `contract_address:token_id`)"
    )]
    ContractLevelNft {
        token_key: String,
        contract_type: String,
    },
    #[error("unknown contract type `{0}`")]
    UnknownContractType(String),
}

/// Maps a token row into the GraphQL token representation.
///
/// Fails for ERC721/ERC1155 contract-level rows, which don't describe an individual token and
/// can't be represented by the `ERC721__Token`/`ERC1155__Token` types.
pub(crate) fn map_token(columns: &TokenColumns<'_>) -> Result<ErcTokenType, TokenMappingError> {
    let contract_type = columns.contract_type.to_lowercase();
    if contract_type == "erc20" {
        return Ok(ErcTokenType::Erc20(Erc20Token {
            contract_address: columns.contract_address.to_owned(),
            name: columns.name.to_owned(),
            symbol: columns.symbol.to_owned(),
            decimals: columns.decimals,
            amount: columns.amount.to_owned(),
        }));
    }

    if contract_type != "erc721" && contract_type != "erc1155" {
        return Err(TokenMappingError::UnknownContractType(
            columns.contract_type.to_owned(),
        ));
    }

    let Some((contract_address, token_id)) = columns.token_key.split_once(':') else {
        return Err(TokenMappingError::ContractLevelNft {
            token_key: columns.token_key.to_owned(),
            contract_type: columns.contract_type.to_owned(),
        });
    };
    let metadata = NftMetadataFields::parse(columns.metadata, contract_address, token_id);

    Ok(if contract_type == "erc721" {
        ErcTokenType::Erc721(Erc721Token {
            name: columns.name.to_owned(),
            symbol: columns.symbol.to_owned(),
            token_id: token_id.to_owned(),
            contract_address: columns.contract_address.to_owned(),
            metadata: columns.metadata.to_owned(),
            metadata_name: metadata.name,
            metadata_description: metadata.description,
            metadata_attributes: metadata.attributes,
            image_path: metadata.image_path,
        })
    } else {
        ErcTokenType::Erc1155(Erc1155Token {
            name: columns.name.to_owned(),
            symbol: columns.symbol.to_owned(),
            token_id: token_id.to_owned(),
            contract_address: columns.contract_address.to_owned(),
            amount: columns.amount.to_owned(),
            metadata: columns.metadata.to_owned(),
            metadata_name: metadata.name,
            metadata_description: metadata.description,
            metadata_attributes: metadata.attributes,
            image_path: metadata.image_path,
        })
    })
}

/// Fields derived from the JSON metadata of an individual NFT.
#[derive(Debug, Default)]
struct NftMetadataFields {
    name: Option<String>,
    description: Option<String>,
    attributes: Option<String>,
    image_path: String,
}

impl NftMetadataFields {
    /// Empty metadata (e.g. a failed metadata fetch, stored as `''`) and metadata that isn't valid
    /// JSON are treated as missing: the derived fields are null and no image path is exposed.
    fn parse(metadata: &str, contract_address: &str, token_id: &str) -> Self {
        if metadata.is_empty() {
            return Self::default();
        }

        let json: serde_json::Value = match serde_json::from_str(metadata) {
            Ok(json) => json,
            Err(error) => {
                debug!(
                    %error,
                    contract_address,
                    token_id,
                    "Token metadata is not valid JSON, ignoring it."
                );
                return Self::default();
            }
        };

        Self {
            name: metadata_field(&json, "name"),
            description: metadata_field(&json, "description"),
            attributes: metadata_field(&json, "attributes"),
            image_path: format!("{contract_address}/{token_id}/image"),
        }
    }
}

/// Returns a metadata field as text: strings verbatim (not JSON-escaped), other non-null values
/// (e.g. the `attributes` array) as JSON.
fn metadata_field(json: &serde_json::Value, key: &str) -> Option<String> {
    match json.get(key)? {
        serde_json::Value::Null => None,
        serde_json::Value::String(value) => Some(value.clone()),
        value => Some(value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns<'a>(
        contract_type: &'a str,
        token_key: &'a str,
        metadata: &'a str,
    ) -> TokenColumns<'a> {
        TokenColumns {
            contract_type,
            token_key,
            contract_address: "0x0abc",
            name: "Name",
            symbol: "SYM",
            decimals: 0,
            metadata,
            amount: "0x2",
        }
    }

    #[test]
    fn contract_level_nft_rows_are_rejected_without_panicking() {
        for contract_type in ["ERC721", "ERC1155"] {
            let err = map_token(&columns(contract_type, "0x0abc", "")).unwrap_err();
            assert!(matches!(err, TokenMappingError::ContractLevelNft { .. }));
        }
    }

    #[test]
    fn erc20_rows_map_without_token_id() {
        let token = map_token(&columns("ERC20", "0x0abc", "")).unwrap();
        assert!(
            matches!(token, ErcTokenType::Erc20(Erc20Token { ref amount, .. }) if amount == "0x2")
        );
    }

    #[test]
    fn unknown_contract_type_is_an_error() {
        let err = map_token(&columns("WORLD", "0x0abc", "")).unwrap_err();
        assert!(matches!(err, TokenMappingError::UnknownContractType(_)));
    }

    #[test]
    fn metadata_strings_are_not_json_escaped() {
        let metadata = r#"{"name":"a \"b\"\nc","description":null,"attributes":[{"trait_type":"x","value":1}]}"#;
        let ErcTokenType::Erc721(token) =
            map_token(&columns("ERC721", "0x0abc:0x01", metadata)).unwrap()
        else {
            panic!("expected an ERC721 token");
        };

        assert_eq!(token.token_id, "0x01");
        assert_eq!(token.metadata_name.as_deref(), Some("a \"b\"\nc"));
        assert_eq!(token.metadata_description, None);
        assert_eq!(
            token.metadata_attributes.as_deref(),
            Some(r#"[{"trait_type":"x","value":1}]"#)
        );
        assert_eq!(token.image_path, "0x0abc/0x01/image");
        assert_eq!(token.metadata, metadata);
    }

    #[test]
    fn empty_or_invalid_metadata_is_treated_as_missing() {
        for metadata in ["", "not json"] {
            for contract_type in ["ERC721", "ERC1155"] {
                let (name, image_path, raw) =
                    match map_token(&columns(contract_type, "0x0abc:0x01", metadata)).unwrap() {
                        ErcTokenType::Erc721(t) => (t.metadata_name, t.image_path, t.metadata),
                        ErcTokenType::Erc1155(t) => (t.metadata_name, t.image_path, t.metadata),
                        ErcTokenType::Erc20(_) => panic!("expected an NFT"),
                    };
                assert_eq!(name, None);
                assert_eq!(image_path, "");
                assert_eq!(raw, metadata);
            }
        }
    }
}
