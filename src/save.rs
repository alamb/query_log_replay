use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use arrow::json::ArrayWriter;
use structopt::StructOpt;

use influxdb_iox_client::connection::Connection;

use crate::error::StringifyError;
pub type Result<T, E = String> = std::result::Result<T, E>;

/// Save the contents of `system.queries` to a JSON formatted file.
#[derive(Debug, StructOpt)]
pub struct Save {
    /// The database name
    db: String,

    /// The filename to save the queries to
    filename: String,
}

/// SQL query that is persisted using json
const SQL: &str = "select * from system.queries";

impl Save {
    pub async fn execute(&self, connection: Connection) -> Result<()> {
        println!(
            "Saving queries from database {} to {}...",
            self.db, self.filename
        );

        let path = Path::new(&self.filename);
        let file = File::create(&path).context(&format!("Creating file {:?}", path))?;
        let mut file = BufWriter::new(file);

        let mut client = influxdb_iox_client::flight::Client::new(connection);

        println!("Running SQL query: '{}'", SQL);

        let mut result = client.perform_query(&self.db, SQL).await.stringify()?;

        let batches = result
            .collect()
            .await
            .context(&format!("Running query {}", SQL))?;

        let mut writer = ArrayWriter::new(&mut file);
        writer
            .write_batches(&batches)
            .context("writing batches as json")?;
        writer.finish().context("completing json-ification")?;
        std::mem::drop(writer);
        file.flush().context("Flushing output buffer")?;

        Ok(())
    }
}
