use clap::Parser;
use influxdb3_client::Client;
use observability_deps::tracing::debug;
use secrecy::ExposeSecret;
use serde::Deserialize;

use super::common::{Format, InfluxDb3Config};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("client error: {0}")]
    InfluxDB3Client(#[from] influxdb3_client::Error),

    #[error("deserializing show tables: {0}")]
    DeserializingShowTables(#[source] serde_json::Error),

    #[error("deserializing show columns: {0}")]
    DeserializingShowColumns(#[source] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Parser)]
#[clap(visible_alias = "s", trailing_var_arg = true)]
pub struct Config {
    #[clap(subcommand)]
    subcommand: SubCommand,

    /// Common InfluxDB 3 Core config
    #[clap(flatten)]
    core_config: InfluxDb3Config,
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// List available system tables for the connected host.
    List(ListConfig),
    /// Retrieve entries from a specific system table.
    Get(GetConfig),
    /// Summarize various types of system table data.
    Summary(SummaryConfig),
}

pub async fn command(config: Config) -> Result<()> {
    let mut client = Client::new(config.core_config.host_url.clone())?;
    if let Some(token) = config
        .core_config
        .auth_token
        .as_ref()
        .map(ExposeSecret::expose_secret)
    {
        client = client.with_auth_token(token);
    }

    let runner = SystemCommandRunner {
        client,
        db: config.core_config.database_name.clone(),
    };
    match config.subcommand {
        SubCommand::Get(cfg) => runner.get(cfg).await,
        SubCommand::List(cfg) => runner.list(cfg).await,
        SubCommand::Summary(cfg) => runner.summary(cfg).await,
    }
}

struct SystemCommandRunner {
    client: Client,
    db: String,
}

#[derive(Debug, Deserialize)]
struct ShowTablesRow {
    //table_catalog: String,
    //table_schema: String,
    table_name: String,
    //table_type: String,
}

#[derive(Debug, Deserialize)]
struct ShowColumnsRow {
    column_name: String,
    //data_type: String,
}

#[derive(Debug, Parser)]
pub struct ListConfig {}

impl SystemCommandRunner {
    async fn list(&self, _config: ListConfig) -> Result<()> {
        for row in self.get_system_tables().await? {
            let columns = self.get_table_columns(row.table_name.as_str()).await?;
            self.display_table(&row, &columns);
        }

        Ok(())
    }

    fn display_table(&self, table: &ShowTablesRow, columns: &[ShowColumnsRow]) {
        println!("{}", table.table_name);
        for c in columns {
            println!("  {}", c.column_name);
        }
    }

    async fn get_system_tables(&self) -> Result<Vec<ShowTablesRow>> {
        let bs = self
            .client
            .api_v3_query_sql(
                self.db.as_str(),
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'system'",
            )
            .format(Format::Json.into())
            .send()
            .await?;

        serde_json::from_slice::<Vec<ShowTablesRow>>(bs.as_ref())
            .map_err(Error::DeserializingShowTables)
    }

    async fn get_table_columns(&self, table_name: &str) -> Result<Vec<ShowColumnsRow>> {
        let bs = self
            .client
            .api_v3_query_sql(self.db.as_str(), format!("SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"))
            .format(Format::Json.into())
            .send()
            .await?;

        serde_json::from_slice(bs.as_ref()).map_err(Error::DeserializingShowColumns)
    }
}

#[derive(Debug, Parser)]
pub struct GetConfig {
    /// The system table to query.
    system_table: String,

    /// The maximum number of table entries to display in the output. Default is 100 and 0 can be
    /// passed to indicate no limit.
    #[clap(long = "limit", short = 'l', default_value_t = 100)]
    limit: u16,

    /// Order by the specified fields.
    #[clap(long = "order-by", short = 'o', num_args = 1, value_delimiter = ',')]
    order_by: Vec<String>,

    /// Select specified fields from table.
    #[clap(long = "select", short = 's', num_args = 1, value_delimiter = ',')]
    select: Vec<String>,

    /// The format in which to output the query
    ///
    /// If `--format` is set to `parquet`, then you must also specify an output
    /// file path with `--output`.
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,
}

impl SystemCommandRunner {
    async fn get(&self, config: GetConfig) -> Result<()> {
        let Self { client, db } = self;
        let GetConfig {
            system_table,
            limit,
            select,
            order_by,
            output_format,
        } = &config;

        let select_expr = if !select.is_empty() {
            select.join(",")
        } else {
            "*".to_string()
        };

        let mut clauses = vec![format!("SELECT {select_expr} FROM system.{system_table}")];

        if !order_by.is_empty() {
            clauses.push(format!("ORDER BY {}", order_by.join(",")));
        } else if let Some(default_ordering) = default_ordering(system_table) {
            clauses.push(format!("ORDER BY {default_ordering}"));
        }
        if *limit > 0 {
            clauses.push(format!("LIMIT {limit}"));
        }

        let query = clauses.join("\n");
        println!("{query}");

        let bs = client
            .api_v3_query_sql(db, query)
            .format(output_format.clone().into())
            .send()
            .await?;

        println!("{}", String::from_utf8(bs.as_ref().to_vec()).unwrap());

        Ok(())
    }
}

#[derive(Debug, Parser)]
pub struct SummaryConfig {
    /// The maximum number of entries from each table to display in the output. Default is 10 and 0
    /// can be passed to indicate no limit.
    #[clap(long = "limit", short = 'l', default_value_t = 10)]
    limit: u16,

    /// The format in which to output the query
    ///
    /// If `--format` is set to `parquet`, then you must also specify an output
    /// file path with `--output`.
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,
}

impl SystemCommandRunner {
    async fn summary(&self, config: SummaryConfig) -> Result<()> {
        self.summarize_all_tables(config.limit, &config.output_format)
            .await?;
        Ok(())
    }

    async fn summarize_all_tables(&self, limit: u16, format: &Format) -> Result<()> {
        let system_tables = self.get_system_tables().await?;
        for table in system_tables {
            self.summarize_table(table.table_name.as_str(), limit, format)
                .await?;
        }
        Ok(())
    }

    async fn summarize_table(&self, table_name: &str, limit: u16, format: &Format) -> Result<()> {
        let Self { db, client } = self;
        let mut clauses = vec![format!("SELECT * FROM system.{table_name}")];

        if let Some(default_ordering) = default_ordering(table_name) {
            clauses.push(format!("ORDER BY {default_ordering}"));
        }

        if limit > 0 {
            clauses.push(format!("LIMIT {limit}"));
        }

        let query = clauses.join("\n");
        debug!("{query}");

        let bs = client
            .api_v3_query_sql(db, query)
            .format(format.clone().into())
            .send()
            .await?;

        println!("{table_name} summary:");
        println!("{}", String::from_utf8(bs.as_ref().to_vec()).unwrap());
        Ok(())
    }
}

fn default_ordering(table_name: &str) -> Option<String> {
    match table_name {
        "cpu" => Some("usage_percent"),
        "last_caches" => Some("count"),
        "parquet_files" => Some("size_bytes"),
        "queries" => Some("end2end_duration"),
        "distinct_caches" => Some("max_cardinality"),
        _ => None,
    }
    .map(ToString::to_string)
}
