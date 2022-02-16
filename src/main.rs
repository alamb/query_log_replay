use std::process::exit;

use structopt::StructOpt;
mod compact;
pub mod error;
mod load;
pub(crate) mod query;
pub(crate) mod query_log;
mod replay;
mod save;
mod util;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "query_log_replay",
    version = "0.1.0",
    about = "InfluxDB IOx server and command line tools",
    long_about = r#"
Save the contents of the `system.queries` system table
to / from files and then replay them against other IOx servers

Ideally this could be done using a tool like `grpcurl` but at the
time of writing there were issues with how the rust `pbjson`
library encoded `Any`, which made writing our own custom handler
easier.

Examples:
    # Save query logs to a file (queries.json)
    influxdb_iox database query my_db 'select * from system.queries' --format=json > queries.json
    # or
    query_log_replay --host http://localhost:8082 save my_db queries.json

    # replay the queries in queries.json back against my_db
    query_log_replay --host http://localhost:8082 replay my_db queries.json

"#
)]
struct Config {
    /// gRPC address of IOx server to connect to
    #[structopt(
        short,
        long,
        global = true,
        env = "IOX_ADDR",
        default_value = "http://127.0.0.1:8082"
    )]
    host: String,

    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    Save(save::Save),
    Replay(replay::Replay),
    LoadReadBuffer(load::LoadReadBuffer),
    FullyCompact(compact::FullyCompact),
}

#[tokio::main]
async fn main() {
    let config: Config = StructOpt::from_args();

    println!("InfluxDB IOx Query Replay Tool... online");

    println!("Connecting to {}", config.host);
    let connection = influxdb_iox_client::connection::Builder::new()
        .build(&config.host)
        .await
        .expect("Can not connect");

    let command_result = match config.command {
        Command::Save(s) => s.execute(connection).await,
        Command::Replay(r) => r.execute(connection).await,
        Command::LoadReadBuffer(lrb) => lrb.execute(connection).await,
        Command::FullyCompact(fc) => fc.execute(connection).await,
    };

    match command_result {
        Ok(_) => {
            println!("Success");
            exit(0)
        }
        Err(e) => {
            println!("Failure: {}", e);
            exit(-1);
        }
    }
}
