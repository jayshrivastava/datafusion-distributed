//! Release-only local matrix runner; see the adjacent plan.md.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::ipc::CompressionType;
use arrow::util::display::array_value_to_string;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_datafusion_err, exec_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::collect;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use datafusion_distributed::test_utils::localhost::{
    LocalHostWorkerResolver, spawn_worker_service,
};
use datafusion_distributed::{
    DefaultSessionBuilder, DistributedExt, DistributedMetricsFormat, SessionStateBuilderExt,
    display_plan_ascii, rewrite_distributed_plan_with_dynamic_filters,
    rewrite_distributed_plan_with_metrics,
};
use datafusion_distributed_benchmarks::datasets::{clickbench, tpcds, tpch};
use serde::Serialize;
use structopt::StructOpt;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

const WORKERS: usize = 4;
const PARTITIONS: usize = 4;

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(long)]
    data: PathBuf,
    #[structopt(long)]
    output: PathBuf,
    #[structopt(long, default_value = "tpch")]
    suite: String,
    #[structopt(long, default_value = "q1", use_delimiter = true)]
    queries: Vec<String>,
    #[structopt(long, default_value = "3")]
    iterations: usize,
    #[structopt(
        long,
        default_value = "Control-off,A-off,Control-on,A-on",
        use_delimiter = true
    )]
    cases: Vec<String>,
    #[structopt(long)]
    reverse_order: bool,
    #[structopt(long, default_value = "180")]
    timeout_seconds: u64,
    #[structopt(long)]
    worker: Option<u16>,
}

#[derive(Clone, Serialize)]
struct Case {
    name: String,
    dynamic_filtering: bool,
    pushdown_filters: bool,
    reorder_filters: bool,
}

impl Case {
    fn parse(name: &str) -> Result<Self> {
        let Some((build, mode)) = name.split_once('-') else {
            return exec_err!("Invalid case: {name}");
        };
        if !matches!(build, "Control" | "A" | "B") || !matches!(mode, "off" | "on") {
            return exec_err!("Invalid case: {name}");
        }
        Ok(Self {
            name: name.to_owned(),
            dynamic_filtering: build != "Control",
            pushdown_filters: mode == "on",
            reorder_filters: mode == "on",
        })
    }
}

#[derive(Serialize)]
struct Sample {
    query: String,
    case: Case,
    iteration: usize,
    statement: usize,
    planning_ms: f64,
    execution_ms: f64,
    types: Vec<String>,
    rows: Vec<Vec<String>>,
    plan: String,
    display_error: Option<String>,
    metrics: Vec<NodeMetrics>,
}

#[derive(Serialize)]
struct NodeMetrics {
    index: usize,
    node: String,
    columns: Vec<String>,
    dynamic_filter: bool,
    values: BTreeMap<String, usize>,
}

fn main() -> Result<()> {
    let options = Options::from_args();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(if options.worker.is_some() {
            PARTITIONS
        } else {
            2
        })
        .enable_all()
        .build()?
        .block_on(async {
            if let Some(port) = options.worker {
                spawn_worker_service(
                    DefaultSessionBuilder,
                    TcpListener::bind(("127.0.0.1", port)).await?,
                )
                .await?;
                return Ok(());
            }
            if cfg!(debug_assertions) || options.iterations == 0 {
                return exec_err!("Use --release and a positive iteration count");
            }
            fs::create_dir_all(options.output.join("plans"))?;
            fs::write(
                options.output.join("options.txt"),
                format!("{options:#?}\n"),
            )?;
            let (mut children, ports) = workers(&options).await?;
            let result = benchmark(&options, &ports).await;
            for child in &mut children {
                let _ = child.kill().await;
                let _ = child.wait().await;
            }
            result
        })
}

async fn workers(options: &Options) -> Result<(Vec<Child>, Vec<u16>)> {
    let mut children = Vec::new();
    let mut ports = Vec::new();
    for index in 0..WORKERS {
        let reservation = TcpListener::bind("127.0.0.1:0").await?;
        let port = reservation.local_addr()?.port();
        drop(reservation);
        let log = File::create(options.output.join(format!("worker-{index}.log")))?;
        let mut child = Command::new(std::env::current_exe()?)
            .arg("--data")
            .arg(&options.data)
            .arg("--output")
            .arg(&options.output)
            .args(["--worker", &port.to_string()])
            .stdout(log.try_clone()?)
            .stderr(log)
            .kill_on_drop(true)
            .spawn()?;
        let deadline = Instant::now() + Duration::from_secs(30);
        while TcpStream::connect(("127.0.0.1", port)).await.is_err() {
            if let Some(status) = child.try_wait()? {
                return exec_err!("Worker {index} exited with {status}");
            }
            if Instant::now() >= deadline {
                return exec_err!("Worker {index} startup timed out");
            }
            sleep(Duration::from_millis(20)).await;
        }
        ports.push(port);
        children.push(child);
    }
    Ok((children, ports))
}

async fn context(case: &Case, data: &Path, ports: &[u16]) -> Result<SessionContext> {
    let mut config = SessionConfig::new().with_target_partitions(PARTITIONS);
    config.options_mut().set(
        "datafusion.optimizer.enable_dynamic_filter_pushdown",
        &case.dynamic_filtering.to_string(),
    )?;
    config.options_mut().execution.parquet.pushdown_filters = case.pushdown_filters;
    config.options_mut().execution.parquet.reorder_filters = case.reorder_filters;
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_distributed_planner()
        .with_distributed_worker_resolver(LocalHostWorkerResolver::new(ports.iter().copied()))
        .with_distributed_max_tasks_per_stage(WORKERS)?
        .with_distributed_dynamic_task_count(false)?
        .with_distributed_broadcast_joins(true)?
        .with_distributed_children_isolator_unions(true)?
        .with_distributed_compression(Some(CompressionType::LZ4_FRAME))?
        .with_distributed_metrics_collection(true)?
        .with_distributed_dynamic_filter_collection(true)?
        .build();
    let ctx = SessionContext::new_with_state(state);
    for entry in fs::read_dir(data)? {
        let path = entry?.path();
        if path.is_dir() {
            let path = fs::canonicalize(path)?;
            let name = path.file_name().unwrap().to_string_lossy();
            ctx.register_parquet(
                name.as_ref(),
                &path.to_string_lossy(),
                ParquetReadOptions::default(),
            )
            .await?;
        }
    }
    Ok(ctx)
}

async fn benchmark(options: &Options, ports: &[u16]) -> Result<()> {
    let cases = options
        .cases
        .iter()
        .map(|name| Case::parse(name))
        .collect::<Result<Vec<_>>>()?;
    let mut contexts = Vec::new();
    for case in &cases {
        contexts.push(context(case, &options.data, ports).await?);
    }
    for query in &options.queries {
        let sql = match options.suite.as_str() {
            "tpch" => tpch::get_query(query)?,
            "tpcds" => tpcds::get_query(query)?,
            "clickbench" => clickbench::get_query(query)?,
            other => return exec_err!("Unknown suite: {other}"),
        };
        fs::write(options.output.join(format!("{query}.sql")), &sql)?;
        let mut failed = vec![false; cases.len()];
        for iteration in 0..=options.iterations {
            let mut order: Vec<_> = (0..cases.len())
                .map(|i| (i + iteration) % cases.len())
                .collect();
            if (iteration / cases.len()) % 2 == 1 {
                order.reverse();
            }
            if options.reverse_order {
                order.reverse();
            }
            for index in order {
                if failed[index] {
                    continue;
                }
                let case = &cases[index];
                let result = timeout(
                    Duration::from_secs(options.timeout_seconds),
                    execute(options, &contexts[index], query, &sql, case, iteration),
                )
                .await;
                let error = match result {
                    Ok(Ok(())) => continue,
                    Ok(Err(error)) => error.to_string(),
                    Err(error) => format!("Timed out after {}s: {error}", options.timeout_seconds),
                };
                eprintln!("ERROR {query} {} iteration={iteration}: {error}", case.name);
                fs::write(
                    options
                        .output
                        .join(format!("{query}-{}-error.json", case.name)),
                    serde_json::to_vec_pretty(&serde_json::json!({
                        "query": query, "case": case, "iteration": iteration, "error": error,
                    }))
                    .map_err(|error| exec_datafusion_err!("Cannot save error: {error}"))?,
                )?;
                failed[index] = true;
            }
        }
    }
    Ok(())
}

async fn execute(
    options: &Options,
    ctx: &SessionContext,
    query: &str,
    sql: &str,
    case: &Case,
    iteration: usize,
) -> Result<()> {
    for (statement_index, statement) in DFParser::parse_sql(sql)?.into_iter().enumerate() {
        let is_query = matches!(&statement, Statement::Statement(s) if matches!(s.as_ref(), SqlStatement::Query(_)));
        if !is_query {
            ctx.sql(&statement.to_string()).await?.collect().await?;
            continue;
        }
        let start = Instant::now();
        let plan = ctx
            .sql(&statement.to_string())
            .await?
            .create_physical_plan()
            .await?;
        let planning_ms = start.elapsed().as_secs_f64() * 1000.0;
        let start = Instant::now();
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        let execution_ms = start.elapsed().as_secs_f64() * 1000.0;
        let types = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.data_type().to_string())
            .collect();
        let mut rows = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|a| array_value_to_string(a.as_ref(), row))
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                );
            }
        }
        rows.sort();
        let (plan, display_error) =
            match rewrite_distributed_plan_with_dynamic_filters(Arc::clone(&plan), &ctx.task_ctx())
                .await
            {
                Ok(plan) => (plan, None),
                Err(error) => (plan, Some(error.to_string())),
            };
        let plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::PerTask).await?;
        let mut metrics = Vec::new();
        plan.apply(|node| {
            let mut values = BTreeMap::new();
            if let Some(set) = node.metrics() {
                for metric in set.iter() {
                    let value = metric.value();
                    *values.entry(value.name().to_owned()).or_default() += value.as_usize();
                }
            }
            let mut dynamic_filter = false;
            node.apply_expressions(&mut |expr| {
                dynamic_filter |= expr.exists(|expr| Ok(expr.is::<DynamicFilterPhysicalExpr>()))?;
                Ok(TreeNodeRecursion::Continue)
            })?;
            metrics.push(NodeMetrics {
                index: metrics.len(),
                node: node.name().to_owned(),
                dynamic_filter,
                columns: node
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect(),
                values,
            });
            Ok(TreeNodeRecursion::Continue)
        })?;
        let stem = format!("{query}-{}-{iteration:02}-s{statement_index}", case.name);
        let plan_file = format!("plans/{stem}.txt");
        fs::write(
            options.output.join(&plan_file),
            display_plan_ascii(plan.as_ref(), true),
        )?;
        println!(
            "{query} {} iteration={iteration} statement={statement_index} execution_ms={execution_ms:.2} planning_ms={planning_ms:.2} rows={}",
            case.name,
            rows.len()
        );
        let sample = Sample {
            query: query.to_owned(),
            case: case.clone(),
            iteration,
            statement: statement_index,
            planning_ms,
            execution_ms,
            types,
            rows,
            plan: plan_file,
            display_error,
            metrics,
        };
        fs::write(
            options.output.join(format!("{stem}.json")),
            serde_json::to_vec_pretty(&sample)
                .map_err(|error| exec_datafusion_err!("Cannot save sample: {error}"))?,
        )?;
    }
    Ok(())
}
