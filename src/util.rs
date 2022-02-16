use std::time::Duration;

use influxdb_iox_client::{connection::Connection, google::longrunning::IoxOperation};

pub type Result<T, E = String> = std::result::Result<T, E>;
use crate::error::StringifyError;

const MAX_OPERATION_WAIT_SECS: u64 = 10;

/// Wait for all operations listed in `jobs` to complete, with status reporting
pub async fn wait_for_jobs(connection: Connection, jobs: Vec<IoxOperation>) -> Result<()> {
    if jobs.is_empty() {
        return Ok(());
    }

    let mut operation_client = influxdb_iox_client::operations::Client::new(connection);
    print!("Waiting for {} jobs to complete", jobs.len());
    for (counter, job) in jobs.into_iter().enumerate() {
        let id = job.operation.id();
        let timeout = Duration::from_secs(MAX_OPERATION_WAIT_SECS);
        operation_client
            .wait_operation(id, Some(timeout))
            .await
            .context(&format!("waiting for operation to complete:{:#?}", job))?;
        print!(".");
        if (counter % 10) == 0 {
            print!("{}", counter);
        }
    }
    println!(" Done");

    Ok(())
}
