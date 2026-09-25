//! SF10 dynamic-filter matrix with separate localhost workers and executed plans.

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
use datafusion::physical_plan::collect;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_distributed::test_utils::localhost::{
    LocalHostWorkerResolver, spawn_worker_service,
};
use datafusion_distributed::{
    DefaultSessionBuilder, DistributedExt, DistributedMetricsFormat, SessionStateBuilderExt,
    display_plan_ascii, rewrite_distributed_plan_with_dynamic_filters,
    rewrite_distributed_plan_with_metrics,
};
use datafusion_distributed_benchmarks::datasets::tpch;
use serde::Serialize;
use structopt::StructOpt;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

const WORKERS: usize = 4;
const PARTITIONS: usize = 4;
const QUERIES: [&str; 4] = ["q17", "q18", "q20", "q15"];

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(long)]
    data: PathBuf,
    #[structopt(long)]
    output: PathBuf,
    #[structopt(long, default_value = "10")]
    iterations: usize,
    #[structopt(long)]
    worker: Option<u16>,
}

#[derive(Clone, Copy, Serialize)]
struct Case {
    name: &'static str,
    dynamic_filtering: bool,
    pushdown_filters: bool,
    reorder_filters: bool,
}

const CASES: [Case; 3] = [
    Case {
        name: "all_off",
        dynamic_filtering: false,
        pushdown_filters: false,
        reorder_filters: false,
    },
    Case {
        name: "dynamic_only",
        dynamic_filtering: true,
        pushdown_filters: false,
        reorder_filters: false,
    },
    Case {
        name: "all_on",
        dynamic_filtering: true,
        pushdown_filters: true,
        reorder_filters: true,
    },
];

#[derive(Serialize)]
struct Sample {
    query: String,
    case: Case,
    iteration: usize,
    planning_ms: f64,
    execution_ms: f64,
    rows: Vec<Vec<String>>,
    plan: String,
    metrics: Vec<NodeMetrics>,
}

#[derive(Serialize)]
struct NodeMetrics {
    node: String,
    columns: Vec<String>,
    values: BTreeMap<String, usize>,
}

fn main() -> Result<()> {
    let options = Options::from_args();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(if options.worker.is_some() {
            PARTITIONS
        } else {
            2
        })
        .enable_all()
        .build()?;
    runtime.block_on(async {
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

async fn context(case: Case, data: &Path, ports: &[u16]) -> Result<SessionContext> {
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
    for table in [
        "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
    ] {
        let path = fs::canonicalize(data.join(table))?;
        ctx.register_parquet(
            table,
            &path.to_string_lossy(),
            ParquetReadOptions::default(),
        )
        .await?;
    }
    Ok(ctx)
}

async fn benchmark(options: &Options, ports: &[u16]) -> Result<()> {
    let mut contexts = Vec::new();
    for case in CASES {
        contexts.push(context(case, &options.data, ports).await?);
    }
    let mut samples = Vec::new();
    for query in QUERIES {
        let sql = tpch::get_query(query)?;
        fs::write(options.output.join(format!("{query}.sql")), &sql)?;
        let mut expected = None;
        // Warm up each case once, then rotate/reverse the order to reduce cache bias.
        for iteration in 0..=options.iterations {
            let mut order: Vec<_> = (0..CASES.len())
                .map(|i| (i + iteration) % CASES.len())
                .collect();
            if (iteration / CASES.len()) % 2 == 1 {
                order.reverse();
            }
            for index in order {
                let case = CASES[index];
                let ctx = &contexts[index];
                for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                    if statement.starts_with("create") || statement.starts_with("drop") {
                        ctx.sql(statement).await?.collect().await?;
                        continue;
                    }
                    let start = Instant::now();
                    let plan = ctx.sql(statement).await?.create_physical_plan().await?;
                    let planning_ms = start.elapsed().as_secs_f64() * 1000.0;
                    let start = Instant::now();
                    let batches = timeout(
                        Duration::from_secs(600),
                        collect(Arc::clone(&plan), ctx.task_ctx()),
                    )
                    .await
                    .map_err(|error| exec_datafusion_err!("{query} timed out: {error}"))??;
                    let execution_ms = start.elapsed().as_secs_f64() * 1000.0;
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
                    if let Some(expected) = &expected {
                        if &rows != expected {
                            return exec_err!(
                                "{query} {} iteration {iteration}: result differs from all_off",
                                case.name
                            );
                        }
                    } else {
                        expected = Some(rows.clone());
                    }
                    let plan = rewrite_distributed_plan_with_dynamic_filters(plan, &ctx.task_ctx())
                        .await?;
                    let plan = rewrite_distributed_plan_with_metrics(
                        plan,
                        DistributedMetricsFormat::PerTask,
                    )
                    .await?;
                    let mut metrics = Vec::new();
                    plan.apply(|node| {
                        if let Some(set) = node.metrics() {
                            let mut values = BTreeMap::new();
                            for metric in set.iter() {
                                let value = metric.value();
                                let name = value.name();
                                if name.contains("rows")
                                    || name.contains("bytes")
                                    || name.contains("prun")
                                    || name.contains("dynamic_filter")
                                    || name == "row_pushdown_eval_time"
                                    || name == "elapsed_compute"
                                {
                                    *values.entry(name.to_owned()).or_default() += value.as_usize();
                                }
                            }
                            metrics.push(NodeMetrics {
                                node: node.name().to_owned(),
                                columns: node
                                    .schema()
                                    .fields()
                                    .iter()
                                    .map(|f| f.name().clone())
                                    .collect(),
                                values,
                            });
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                    let plan_file = format!("plans/{query}-{}-{iteration:02}.txt", case.name);
                    fs::write(
                        options.output.join(&plan_file),
                        display_plan_ascii(plan.as_ref(), true),
                    )?;
                    println!(
                        "{query} {} iteration={iteration} execution_ms={execution_ms:.2} planning_ms={planning_ms:.2} rows={}",
                        case.name,
                        rows.len()
                    );
                    samples.push(Sample {
                        query: query.to_owned(),
                        case,
                        iteration,
                        planning_ms,
                        execution_ms,
                        rows,
                        plan: plan_file,
                        metrics,
                    });
                    fs::write(
                        options.output.join("samples.json"),
                        serde_json::to_vec_pretty(&samples).map_err(|error| {
                            exec_datafusion_err!("Cannot save samples: {error}")
                        })?,
                    )?;
                }
            }
        }
    }
    Ok(())
}
