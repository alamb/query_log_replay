use std::{
    fmt::{Display, Formatter},
    time::{Duration, Instant},
};

use crate::error::StringifyError;
use futures::stream::TryStreamExt;
use generated_types::influxdata::platform::storage::{
    storage_client::StorageClient, ReadFilterRequest,
};
use generated_types::prost::Message;
use generated_types::ReadSource;
use influxdb_iox_client::connection::Connection;

pub type Result<T, E = String> = std::result::Result<T, E>;

/// The type of RPC that was encoded in this log
/// The values are pbjson formatted rpc requests
#[derive(Debug, Clone)]
pub enum StorageRpc {
    ReadFilter(ReadFilterRequest),
}

impl Display for StorageRpc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (org_id, bucket_id) = self
            .read_source()
            .ok()
            .unwrap_or_else(|| ("UNKNOWN".to_string(), "UNKNOWN".to_string()));

        write!(
            f,
            "{}(org_id={}, bucket_id={}, details={})",
            self.name(),
            org_id,
            bucket_id,
            self.details()
        )
    }
}

impl StorageRpc {
    pub fn name(&self) -> &'static str {
        match self {
            StorageRpc::ReadFilter(_) => "ReadFilter",
        }
    }

    pub fn details(&self) -> &'static str {
        match self {
            StorageRpc::ReadFilter(_) => "(Add Predicates)",
        }
    }

    // Return the original org_id and bucket_id for this request
    pub fn read_source(&self) -> Result<(String, String)> {
        let read_source = match self {
            StorageRpc::ReadFilter(request) => request.read_source.as_ref(),
        }
        .ok_or_else(|| format!("No read source found on request {}", self.name()))?;

        let ReadSource {
            bucket_id, org_id, ..
        } = Message::decode(&read_source.value[..]).context(&format!(
            "value could not be parsed as a ReadSource message on request {}",
            self.name()
        ))?;

        Ok((bucket_id.to_string(), org_id.to_string()))
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Query::Sql(sql) => write!(f, "Sql({})", truncate_and_clean(sql, 30)),
            Query::StorageRpc(storagerpc) => write!(f, "StorageRpc({})", storagerpc),
        }
    }
}

/// Information on the results of running a `Query`
#[derive(Default, Debug)]
pub struct QueryExecution {
    /// the total time to run the query (including network time)
    pub duration: Duration,

    /// Total number of rows  returned
    pub num_rows: usize,

    /// Total number of frames returned
    pub num_frames: usize,
}

impl Display for QueryExecution {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} rows {} frames in {:?}",
            self.num_rows, self.num_frames, self.duration
        )
    }
}

impl QueryExecution {
    /// Aggregate the exeuction with other
    fn aggregate(mut self, other: &QueryExecution) -> Self {
        self.duration += other.duration;
        self.num_rows += other.num_rows;
        self.num_frames += other.num_frames;
        self
    }
}

#[derive(Debug, Clone)]
/// thing to help create `QueryExecution`
struct QueryExecutionBuilder {
    start: Instant,
    num_rows: usize,
    num_frames: usize,
}

impl QueryExecutionBuilder {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            num_rows: 0,
            num_frames: 0,
        }
    }

    /// record that the query produced `num_rows` more
    fn add_rows(&mut self, num_rows: usize) {
        self.num_rows += num_rows;
    }

    /// record that the query produced `num_frames` more
    fn add_frames(&mut self, num_frames: usize) {
        self.num_frames += num_frames;
    }

    fn build(self) -> QueryExecution {
        let Self {
            start,
            num_rows,
            num_frames,
        } = self;

        QueryExecution {
            duration: start.elapsed(),
            num_rows,
            num_frames,
        }
    }
}

// Summarize multiple `QueryExecutions`
#[derive(Debug, Default)]
pub struct QueryExecutionSummary {
    /// Total duration, num_rows, and num_frames
    pub inner: QueryExecution,

    /// minimum duration any query took
    pub min_duration: Duration,

    /// maximum duration any query took
    pub max_duration: Duration,

    /// The total number of executions aggregated
    pub count: usize,
}

impl QueryExecutionSummary {
    /// return something that displays headers for query execution summaries
    pub fn header() -> impl Display {
        struct Header {}
        impl Display for Header {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "total_duration_ms\tmin_duration_ms\tmax_duration_ms\tcount\ttotal_rows\ttotal_frames")
            }
        }
        Header {}
    }
}

impl Display for QueryExecutionSummary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}\t{}\t{}\t{}\t{}\t{}",
            self.inner.duration.as_millis(),
            self.min_duration.as_millis(),
            self.max_duration.as_millis(),
            self.count,
            self.inner.num_rows,
            self.inner.num_frames,
        )
    }
}

#[derive(Debug, Default)]
pub struct QueryExecutionSummaryBuilder {
    inner: Option<QueryExecution>,
    min_duration: Option<Duration>,
    max_duration: Option<Duration>,
    count: usize,
}

impl QueryExecutionSummaryBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    /// What is the total duration represented by the executions so far?
    pub fn total_duration(&self) -> Duration {
        self.inner
            .as_ref()
            .map(|execution| execution.duration)
            .unwrap_or_default()
    }

    pub fn build(self) -> QueryExecutionSummary {
        QueryExecutionSummary {
            inner: self.inner.unwrap(),
            min_duration: self.min_duration.unwrap(),
            max_duration: self.max_duration.unwrap(),
            count: self.count,
        }
    }

    /// Add the query execution to the builder
    pub fn add(mut self, summary: QueryExecution) -> Self {
        self.min_duration = Some(
            self.min_duration
                .take()
                .map(|cur_min| {
                    if summary.duration < cur_min {
                        summary.duration
                    } else {
                        cur_min
                    }
                })
                .unwrap_or(summary.duration),
        );

        self.max_duration = Some(
            self.max_duration
                .take()
                .map(|cur_max| {
                    if summary.duration > cur_max {
                        summary.duration
                    } else {
                        cur_max
                    }
                })
                .unwrap_or(summary.duration),
        );

        self.inner = Some(
            self.inner
                .take()
                .map(|cur_inner| cur_inner.aggregate(&summary))
                .unwrap_or_else(|| summary),
        );

        self.count += 1;
        self
    }
}

/// Record of a type of query that was run?
#[derive(Debug, Clone)]
pub enum Query {
    /// SQL for sql type queries,
    Sql(String),
    /// InfluxDB StorageRPC
    StorageRpc(StorageRpc),
}

impl Query {
    ///  create a new Query from the content of the `query_type` and
    ///  `query_text` columns fro a row in `system.queries`
    pub fn try_new(query_type: impl Into<String>, query_text: impl Into<String>) -> Result<Self> {
        let query_type = query_type.into();
        let query_text = query_text.into();

        match query_type.as_str() {
            "sql" => Ok(Self::Sql(query_text)),
            "read_filter" => {
                // parse the payload as a JSON RPC back to the appropriate request type
                let request = serde_json::from_str::<ReadFilterRequest>(&query_text)
                    .context("Error creating read_filter request")?;

                Ok(Self::StorageRpc(StorageRpc::ReadFilter(request)))
            }
            _ => Err(format!("Unsupported query type found: {}", query_type)),
        }
    }

    /// Resend the query to the specfied database name
    pub async fn replay(
        self,
        database_name: &str,
        connection: Connection,
    ) -> Result<QueryExecution> {
        let mut execution = QueryExecutionBuilder::new();

        match self {
            Query::Sql(sql) => {
                let mut client = influxdb_iox_client::flight::Client::new(connection);

                //println!("Running SQL query: '{}'", sql);
                let mut result = client.perform_query(database_name, sql).await.stringify()?;

                while let Some(batch) = result.next().await.stringify()? {
                    //println!("received {} rows", batch.num_rows());
                    execution.add_rows(batch.num_rows());
                }
            }
            Query::StorageRpc(storagerpc) => {
                let mut storage_client = StorageClient::new(connection);
                //println!("Sending storage client request...");Z

                match storagerpc {
                    StorageRpc::ReadFilter(mut request) => {
                        request.with_database(database_name)?;

                        let read_response = storage_client
                            .read_filter(request)
                            .await
                            .context("Error making read_filter request")?;

                        //println!("Got result: {:?}", read_response);
                        let responses: Vec<_> =
                            read_response.into_inner().try_collect().await.unwrap();

                        responses
                            .into_iter()
                            .flat_map(|r| r.frames)
                            .flat_map(|f| f.data)
                            .for_each(|_d| {
                                //println!("Got response data: {:?}", _d);
                                execution.add_frames(1);
                            })
                    }
                }
            }
        }
        Ok(execution.build())
    }
}

fn truncate_and_clean(s: &str, max_chars: usize) -> String {
    let mut s = s.replace('\n', " ");
    s.truncate(max_chars);
    s
}

/// Trait to allow rewriting gRPC requests to target new databases in
/// their `ReadSource` fields
trait WithDatabase {
    /// Modifies this request so its source field targets the specified database
    fn with_database(&mut self, database_name: &str) -> Result<()>;
}

// rewrite the ReadSource to refer to the specified database rather than the original
impl WithDatabase for ReadFilterRequest {
    fn with_database(&mut self, database_name: &str) -> Result<()> {
        self.read_source = make_read_source(database_name)?;
        Ok(())
    }
}

pub fn make_read_source(
    database_name: &str,
) -> Result<Option<generated_types::google::protobuf::Any>> {
    let mut split_name = database_name.split('_');

    let org_id = split_name
        .next()
        .ok_or_else(|| format!("Can not find org name in {}", database_name))
        .and_then(|org_id| {
            u64::from_str_radix(org_id, 16).context(&format!(
                "Can not parse org_id '{}' into u64, required for storage rpc requests",
                org_id
            ))
        })?;

    let bucket_id = split_name
        .next()
        .ok_or_else(|| format!("Can not find bucket name after a '_' in {}", database_name))
        .and_then(|bucket_id| {
            u64::from_str_radix(bucket_id, 16).context(&format!(
                "Can not parse bucket_id '{}' into u64, required for storage rpc requests",
                bucket_id
            ))
        })?;

    if let Some(next) = split_name.next() {
        return Err(format!(
            "Extra trailing content '{}' after org and bucket in {}",
            next, database_name
        ));
    }

    // pick an arbitrary partition id
    let partition_id = u64::from(u32::MAX);
    let read_source = ReadSource {
        org_id,
        bucket_id,
        partition_id,
    };

    let mut d = bytes::BytesMut::new();
    read_source.encode(&mut d).unwrap();
    let read_source = generated_types::google::protobuf::Any {
        type_url: "/TODO".to_string(),
        value: d.freeze(),
    };

    Ok(Some(read_source))
}
