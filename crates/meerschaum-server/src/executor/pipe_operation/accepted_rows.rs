//! Original Main row predicates, distinct from registered-pipe metadata selection.
use crate::error::ApiError;
use meerschaum_core::{Map, Pipe};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowColumn {
    pub dtype: Option<String>,
    pub native_type: Option<String>,
    pub literal_null_marker: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AcceptedRows {
    #[default]
    Unissued,
    All,
    Filter {
        params: Map,
        columns: BTreeMap<String, RowColumn>,
    },
}
fn denied() -> ApiError {
    ApiError::new(
        403,
        "This request differs from Main's accepted row predicate.",
    )
}
impl AcceptedRows {
    pub fn issued(&self) -> bool {
        !matches!(self, Self::Unissued)
    }
    pub fn native(&self) -> Result<meerschaum_connector::mutation::RowPredicate, ApiError> {
        use meerschaum_connector::mutation::{RowColumn as Native, RowPredicate};
        Ok(match self {
            Self::Unissued => {
                return Err(ApiError::unavailable(
                    "Main has not captured this node's row predicate.",
                ));
            }
            Self::All => RowPredicate::All,
            Self::Filter { params, columns } => {
                if params.is_empty()
                    || columns.is_empty()
                    || params.len() > 256
                    || columns.len() > 256
                    || columns.values().any(|column| {
                        column
                            .native_type
                            .as_ref()
                            .is_none_or(|typ| typ.is_empty() || typ.len() > 256)
                    })
                {
                    return Err(denied());
                }
                struct Limit(usize);
                impl std::io::Write for Limit {
                    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                        self.0 = self
                            .0
                            .checked_add(bytes.len())
                            .filter(|size| *size <= 64 * 1024)
                            .ok_or_else(|| std::io::Error::other("row predicate too large"))?;
                        Ok(bytes.len())
                    }
                    fn flush(&mut self) -> std::io::Result<()> {
                        Ok(())
                    }
                }
                serde_json::to_writer(Limit(0), &(params, columns)).map_err(|_| denied())?;
                RowPredicate::Filter {
                    params: params.clone(),
                    columns: columns
                        .iter()
                        .map(|(name, column)| {
                            (
                                name.clone(),
                                Native {
                                    dtype: column.dtype.clone(),
                                    native_type: column.native_type.clone(),
                                    literal_null_marker: column.literal_null_marker,
                                },
                            )
                        })
                        .collect(),
                }
            }
        })
    }
    pub fn request(&self, params: Option<&Map>) -> Result<(), ApiError> {
        match self {
            Self::All => Ok(()),
            Self::Filter {
                params: accepted, ..
            } if params.filter(|params| !params.is_empty()) == Some(accepted) => Ok(()),
            _ => Err(denied()),
        }
    }
    pub fn unchanged(&self, pipe: &Pipe) -> bool {
        match self {
            Self::All => true,
            Self::Unissued => false,
            Self::Filter { columns, .. } => columns
                .iter()
                .all(|(name, column)| pipe.dtype(name) == column.dtype),
        }
    }
}
