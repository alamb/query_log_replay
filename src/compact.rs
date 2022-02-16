use structopt::StructOpt;

use influxdb_iox_client::connection::Connection;

pub type Result<T, E = String> = std::result::Result<T, E>;
use crate::{error::StringifyError, util::wait_for_jobs};

/// Compact all partitions so they have a single chunk
#[derive(Debug, StructOpt)]
pub struct FullyCompact {
    /// The database name for which to load
    db: String,
}

impl FullyCompact {
    pub async fn execute(&self, connection: Connection) -> Result<()> {
        println!("Ensuring all partitions have a single chunk {}", self.db);

        let mut client = influxdb_iox_client::management::Client::new(connection.clone());

        let partitions = client
            .list_partitions(&self.db)
            .await
            .context("Listing partitions")?;

        //println!("{} Available partitions: {:#?}", partitions.len(), partitions);
        let mut jobs = vec![];

        println!("Checking {} Available partitions", partitions.len());
        for partition in partitions {
            let partition_name = format!("Partition({}:{})", partition.table_name, partition.key);
            print!("{} ", partition_name);

            let job = client
                .compact_object_store_partition(&self.db, &partition.table_name, &partition.key)
                .await
                .context("Starting partition compaction")?;
            jobs.push(job);

            println!("scheduled");
        }

        wait_for_jobs(connection, jobs).await
    }
}
