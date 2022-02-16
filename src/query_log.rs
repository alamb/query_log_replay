use std::{fs::File, path::Path};

use serde_json::Value;

use crate::{error::StringifyError, query::Query};

pub type Result<T, E = String> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
/// Represents a row in the system.queries table
pub struct QueryRow {
    /// time at which the query was issued
    _issue_time: String,

    /// Type of the query (TODO parse this into the known types)
    query: Query,
}

impl QueryRow {
    /// Return the contained `Query`
    pub fn into_inner(self) -> Query {
        self.query
    }
}

#[derive(Debug, Clone)]
/// A set of entries from `system.queries`
pub struct QueryLog {
    pub queries: Vec<QueryRow>,
}

// Trait for extracting stuff from a Json::value
trait Extract {
    fn extract_string(self) -> Result<String>;
    fn extract_map(self) -> Result<serde_json::Map<String, Value>>;
    fn extract_array(self) -> Result<Vec<Value>>;
}

impl Extract for Value {
    fn extract_string(self) -> Result<String> {
        if let Value::String(s) = self {
            Ok(s)
        } else {
            Err(format!("Expected an string, got {:?}", self))
        }
    }

    fn extract_map(self) -> Result<serde_json::Map<String, Value>> {
        if let Value::Object(o) = self {
            Ok(o)
        } else {
            Err(format!("Expected an object, got {:?}", self))
        }
    }

    fn extract_array(self) -> Result<Vec<Value>> {
        if let Value::Array(values) = self {
            Ok(values)
        } else {
            Err(format!(
                "Expected json array, but got something else {:?}",
                self
            ))
        }
    }
}

fn get_field(map: &mut serde_json::Map<String, Value>, field_name: &str) -> Result<String> {
    map.remove(field_name)
        .ok_or_else(|| format!("Could not find field {} in value {:?}", field_name, map))?
        .extract_string()
}

impl QueryLog {
    pub async fn new_from_file(path: &Path) -> Result<Self> {
        println!("Loading queries from {:?}", path);
        let file = File::open(path).stringify()?;

        let v: Value = serde_json::from_reader(file).stringify()?;

        // now read record batches out
        let values = v.extract_array()?;

        // each row looks like
        //
        // Object({
        //     "issue_time": String(
        //         "2021-12-16 15:06:22.456268343",
        //     ),
        //     "query_type": String(
        //         "sql",
        //     ),
        //     "query_text": String(
        //         "select count(*), query_type from system.queries group by query_type",
        //     ),
        // }),
        let queries = values
            .into_iter()
            .map(|v| {
                let mut map = v.extract_map()?;
                let query_content = Query::try_new(
                    get_field(&mut map, "query_type")?,
                    get_field(&mut map, "query_text")?,
                )?;

                let query = QueryRow {
                    _issue_time: get_field(&mut map, "issue_time")?,
                    query: query_content,
                };
                Ok(query)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { queries })
    }
}
