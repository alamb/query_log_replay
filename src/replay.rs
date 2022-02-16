use std::{path::Path, time::Duration};

use structopt::StructOpt;

use influxdb_iox_client::connection::Connection;

use crate::{
    query::{QueryExecutionSummary, QueryExecutionSummaryBuilder},
    query_log::QueryLog,
};

pub type Result<T, E = String> = std::result::Result<T, E>;

const TEST_DURATION_SECS: u64 = 5;

/// Replay the contents of previously saved queries from a file back to a databse
#[derive(Debug, StructOpt)]
pub struct Replay {
    /// The database name to replay the queries against
    db: String,

    /// The filename to replay the queries to
    filename: String,
}

impl Replay {
    pub async fn execute(&self, connection: Connection) -> Result<()> {
        println!(
            "Replaying from {} into database {}...",
            self.db, self.filename
        );
        let path = Path::new(&self.filename);

        let log = QueryLog::new_from_file(path).await?;

        println!("Loaded query log with {} entries", log.queries.len());

        // now execute the queries against the specified database and connection
        println!("description,{}", QueryExecutionSummary::header());
        for (i, query) in log.queries.into_iter().map(|r| r.into_inner()).enumerate() {
            let description = query.to_string();
            let mut summary = QueryExecutionSummaryBuilder::new();
            while summary.total_duration() < Duration::from_secs(TEST_DURATION_SECS) {
                let execution = query.clone().replay(&self.db, connection.clone()).await?;
                //println!("Ran {}: {}", description, execution);
                summary = summary.add(execution);
            }
            let summary = summary.build();
            let description = if description.len() > 10 {
                &description.as_str()[0..10]
            } else {
                description.as_str()
            };

            println!("query {}: {},{}", i, description, summary);
        }

        Ok(())
    }
}
