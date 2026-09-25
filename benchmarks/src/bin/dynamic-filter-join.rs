//! Local, selective partitioned-join benchmark with real gRPC worker processes.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, BooleanArray, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_datafusion_err, exec_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_distributed::test_utils::localhost::{
    LocalHostWorkerResolver, spawn_worker_service,
};
use datafusion_distributed::{
    DefaultSessionBuilder, DistributedExt, DistributedMetricsFormat, NetworkShuffleExec,
    SessionStateBuilderExt, display_plan_ascii, rewrite_distributed_plan_with_dynamic_filters,
    rewrite_distributed_plan_with_metrics,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::time::sleep;

const WORKERS: usize = 4;
const PARTITIONS: usize = 4;
const KEYS: usize = 1 << 20;
const FILES: usize = 32;
const BATCH: usize = 65_536;

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(long, default_value = "target/dynamic-filter-join/data")]
    data: PathBuf,
    #[structopt(long, default_value = "target/dynamic-filter-join/results")]
    output: PathBuf,
    #[structopt(long, default_value = "33554432")]
    rows: usize,
    #[structopt(long, default_value = "1024")]
    selected_keys: usize,
    #[structopt(long, default_value = "4")]
    measures: usize,
    #[structopt(long, default_value = "10")]
    iterations: usize,
    /// Override DataFusion's per-partition IN-list limit (zero uses bounds remotely).
    #[structopt(long)]
    inlist_max_values: Option<usize>,
    /// Internal worker mode; the runner chooses unused local ports.
    #[structopt(long)]
    worker: Option<u16>,
}

#[derive(Serialize, Deserialize)]
struct Dataset {
    version: u32,
    rows: usize,
    selected_keys: usize,
    expected: Vec<i64>,
}

#[derive(Clone, Copy, Serialize)]
struct Case {
    name: &'static str,
    dynamic: bool,
    pushdown: bool,
}

const CASES: [Case; 4] = [
    Case {
        name: "off_pruning",
        dynamic: false,
        pushdown: false,
    },
    Case {
        name: "on_pruning",
        dynamic: true,
        pushdown: false,
    },
    Case {
        name: "off_pushdown",
        dynamic: false,
        pushdown: true,
    },
    Case {
        name: "on_pushdown",
        dynamic: true,
        pushdown: true,
    },
];

#[derive(Serialize)]
struct Sample {
    case: Case,
    iteration: usize,
    planning_ms: f64,
    execution_ms: f64,
    result: Vec<i64>,
    metrics: Vec<PlanMetrics>,
}

#[derive(Serialize)]
struct PlanMetrics {
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
            let listener = TcpListener::bind(("127.0.0.1", port)).await?;
            spawn_worker_service(DefaultSessionBuilder, listener).await?;
            return Ok(());
        }
        if cfg!(debug_assertions) {
            return exec_err!("Run this benchmark with --release");
        }
        if options.iterations == 0
            || options.rows == 0
            || !options.rows.is_multiple_of(FILES)
            || options.selected_keys == 0
            || options.selected_keys > KEYS / 2
            || !(1..=64).contains(&options.measures)
        {
            return exec_err!(
                "Use positive iterations, rows divisible by {FILES}, 1..64 measures, and 1..={} selected keys",
                KEYS / 2
            );
        }
        fs::create_dir_all(&options.output)?;
        let dataset = prepare(&options)?;
        fs::write(options.output.join("query.sql"), query(options.measures))?;
        fs::write(options.output.join("settings.txt"), format!("{options:#?}\nworkers={WORKERS}\ntarget_partitions={PARTITIONS}\nreorder_filters=false\n"))?;
        let (mut children, ports) = workers(&options.output).await?;
        let result = benchmark(&options, &dataset, &ports).await;
        for child in &mut children {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        result
    })
}

fn writer(path: &Path, schema: Arc<Schema>) -> Result<ArrowWriter<File>> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(BATCH))
        .build();
    Ok(ArrowWriter::try_new(
        File::create(path)?,
        schema,
        Some(properties),
    )?)
}

fn selected(key: usize, count: usize) -> bool {
    (KEYS / 2..KEYS / 2 + count).contains(&key)
}

fn query(measures: usize) -> String {
    let sums = (0..measures)
        .map(|i| format!("SUM(f.m{i})"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT COUNT(*) AS n, {sums}\nFROM dim d JOIN fact f ON d.k = f.k WHERE d.selected")
}

fn prepare(options: &Options) -> Result<Dataset> {
    let manifest = options.data.join("dataset.json");
    if manifest.exists() {
        let dataset: Dataset = serde_json::from_slice(&fs::read(manifest)?)
            .map_err(|error| exec_datafusion_err!("Invalid dataset manifest: {error}"))?;
        if dataset.version != 2
            || dataset.rows != options.rows
            || dataset.selected_keys != options.selected_keys
            || dataset.expected.len() != options.measures + 1
        {
            return exec_err!("Dataset parameters differ; use a new --data directory");
        }
        return Ok(dataset);
    }
    if options.data.exists() && fs::read_dir(&options.data)?.next().is_some() {
        return exec_err!("Incomplete/nonempty dataset; use a new --data directory");
    }
    fs::create_dir_all(options.data.join("dim"))?;
    fs::create_dir_all(options.data.join("fact"))?;
    println!(
        "Generating {} fact rows, {KEYS} dimension keys, {} selected keys",
        options.rows, options.selected_keys
    );
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("selected", DataType::Boolean, false),
    ]));
    for file in 0..WORKERS {
        let mut out = writer(
            &options.data.join(format!("dim/{file:02}.parquet")),
            Arc::clone(&dim_schema),
        )?;
        for start in (file * KEYS / WORKERS..(file + 1) * KEYS / WORKERS).step_by(BATCH) {
            let end = (start + BATCH).min((file + 1) * KEYS / WORKERS);
            out.write(&RecordBatch::try_new(
                Arc::clone(&dim_schema),
                vec![
                    Arc::new(Int64Array::from_iter_values((start..end).map(|k| k as i64))),
                    Arc::new(BooleanArray::from(
                        (start..end)
                            .map(|k| selected(k, options.selected_keys))
                            .collect::<Vec<_>>(),
                    )),
                ],
            )?)?;
        }
        out.close()?;
    }
    let schema = Arc::new(Schema::new(
        std::iter::once("k".to_owned())
            .chain((0..options.measures).map(|i| format!("m{i}")))
            .map(|name| Field::new(name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let mut expected = vec![0_i64; options.measures + 1];
    for file in 0..FILES {
        let end = (file + 1) * options.rows / FILES;
        let mut out = writer(
            &options.data.join(format!("fact/{file:02}.parquet")),
            Arc::clone(&schema),
        )?;
        for start in (file * options.rows / FILES..end).step_by(BATCH) {
            let mut columns: Vec<Vec<i64>> = (0..=options.measures)
                .map(|_| Vec::with_capacity(BATCH))
                .collect();
            for row in start..(start + BATCH).min(end) {
                // An odd multiplier permutes all keys in each power-of-two-sized cycle.
                let key = row.wrapping_mul(104_729) % KEYS;
                columns[0].push(key as i64);
                let keep = selected(key, options.selected_keys);
                expected[0] += i64::from(keep);
                for column in 1..=options.measures {
                    let value = ((row as u64)
                        .wrapping_mul(0x9e3779b97f4a7c15_u64.wrapping_mul(column as u64))
                        >> 32) as i64;
                    columns[column].push(value);
                    if keep {
                        expected[column] += value;
                    }
                }
            }
            let arrays: Vec<ArrayRef> = columns
                .into_iter()
                .map(|v| Arc::new(Int64Array::from(v)) as ArrayRef)
                .collect();
            out.write(&RecordBatch::try_new(Arc::clone(&schema), arrays)?)?;
        }
        out.close()?;
    }
    let dataset = Dataset {
        version: 2,
        rows: options.rows,
        selected_keys: options.selected_keys,
        expected,
    };
    fs::write(
        manifest,
        serde_json::to_vec_pretty(&dataset)
            .map_err(|error| exec_datafusion_err!("Cannot serialize dataset manifest: {error}"))?,
    )?;
    Ok(dataset)
}

async fn workers(output: &Path) -> Result<(Vec<Child>, Vec<u16>)> {
    let mut children = Vec::new();
    let mut ports = Vec::new();
    for index in 0..WORKERS {
        let reservation = TcpListener::bind("127.0.0.1:0").await?;
        let port = reservation.local_addr()?.port();
        drop(reservation);
        let log = File::create(output.join(format!("worker-{index}.log")))?;
        let mut child = Command::new(std::env::current_exe()?)
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

async fn context(
    case: Case,
    data: &Path,
    ports: &[u16],
    inlist_max_values: Option<usize>,
) -> Result<SessionContext> {
    let mut config = SessionConfig::new().with_target_partitions(PARTITIONS);
    let options = config.options_mut();
    options.set(
        "datafusion.optimizer.enable_dynamic_filter_pushdown",
        &case.dynamic.to_string(),
    )?;
    options.optimizer.hash_join_single_partition_threshold = 0;
    options.optimizer.hash_join_single_partition_threshold_rows = 0;
    if let Some(limit) = inlist_max_values {
        options
            .optimizer
            .hash_join_inlist_pushdown_max_distinct_values = limit;
    }
    options.execution.parquet.pushdown_filters = case.pushdown;
    options.execution.parquet.reorder_filters = false;
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_distributed_planner()
        .with_distributed_worker_resolver(LocalHostWorkerResolver::new(ports.iter().copied()))
        .with_distributed_desired_task_count_handler(WORKERS)
        .with_distributed_max_tasks_per_stage(WORKERS)?
        .with_distributed_dynamic_task_count(false)?
        .with_distributed_broadcast_joins(false)?
        .with_distributed_metrics_collection(true)?
        .with_distributed_dynamic_filter_collection(true)?
        .build();
    let ctx = SessionContext::new_with_state(state);
    for table in ["dim", "fact"] {
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

async fn benchmark(options: &Options, dataset: &Dataset, ports: &[u16]) -> Result<()> {
    let sql = query(options.measures);
    let mut contexts = Vec::new();
    for case in CASES {
        contexts.push(context(case, &options.data, ports, options.inlist_max_values).await?);
    }
    let mut samples = Vec::new();
    // One warm-up per case, then rotate and reverse order to reduce cache/order bias.
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
            let start = Instant::now();
            let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
            let planning_ms = start.elapsed().as_secs_f64() * 1000.0;
            let mut joins = 0;
            plan.apply(|node| {
                if let Some(join) = node.downcast_ref::<HashJoinExec>() {
                    if *join.partition_mode() != PartitionMode::Partitioned
                        || join.left().downcast_ref::<NetworkShuffleExec>().is_none()
                        || join.right().downcast_ref::<NetworkShuffleExec>().is_none()
                    {
                        return exec_err!("Expected a partitioned join with two remote inputs");
                    }
                    joins += 1;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            if joins != 1 {
                return exec_err!("Expected one partitioned join, found {joins}");
            }
            let start = Instant::now();
            let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
            let execution_ms = start.elapsed().as_secs_f64() * 1000.0;
            if batches.len() != 1
                || batches[0].num_rows() != 1
                || batches[0].columns().iter().any(|a| a.null_count() != 0)
            {
                return exec_err!("Expected one non-null result row");
            }
            let result: Vec<i64> = batches[0]
                .columns()
                .iter()
                .map(|a| {
                    a.as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("integer count/sums")
                        .value(0)
                })
                .collect();
            if result != dataset.expected {
                return exec_err!(
                    "{} result {result:?} differs from generated oracle {:?}",
                    case.name,
                    dataset.expected
                );
            }
            let plan = rewrite_distributed_plan_with_dynamic_filters(plan, &ctx.task_ctx()).await?;
            let plan =
                rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                    .await?;
            let mut metrics = Vec::new();
            plan.apply(|node| {
                if let Some(set) = node.metrics() {
                    let mut values = BTreeMap::new();
                    for metric in set.iter() {
                        let value = metric.value();
                        if [
                            "output_rows",
                            "input_rows",
                            "bytes_scanned",
                            "bytes_transferred",
                            "pushdown_rows_pruned",
                            "row_groups_pruned_dynamic_filter",
                            "row_pushdown_eval_time",
                            "dynamic_filter_updates_received",
                        ]
                        .contains(&value.name())
                        {
                            *values.entry(value.name().to_owned()).or_default() += value.as_usize();
                        }
                    }
                    metrics.push(PlanMetrics {
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
            let updates = metrics
                .iter()
                .filter(|m| m.node == "DistributedExec")
                .filter_map(|m| m.values.get("dynamic_filter_updates_received"))
                .sum::<usize>();
            if case.dynamic && updates == 0 {
                return exec_err!("Dynamic filtering was enabled but no remote updates arrived");
            }
            fs::write(
                options
                    .output
                    .join(format!("{}-{iteration:02}.txt", case.name)),
                display_plan_ascii(plan.as_ref(), true),
            )?;
            println!(
                "{} iteration={iteration} execution_ms={execution_ms:.2} planning_ms={planning_ms:.2} result={result:?}",
                case.name
            );
            if iteration != 0 {
                samples.push(Sample {
                    case,
                    iteration,
                    planning_ms,
                    execution_ms,
                    result,
                    metrics,
                });
                fs::write(
                    options.output.join("samples.json"),
                    serde_json::to_vec_pretty(&samples).map_err(|error| {
                        exec_datafusion_err!("Cannot serialize samples: {error}")
                    })?,
                )?;
            }
        }
    }
    for case in CASES {
        let mut times: Vec<_> = samples
            .iter()
            .filter(|s| s.case.name == case.name)
            .map(|s| s.execution_ms)
            .collect();
        times.sort_by(f64::total_cmp);
        let mean = times.iter().sum::<f64>() / times.len() as f64;
        println!(
            "{} mean_ms={mean:.2} median_ms={:.2} min_ms={:.2} max_ms={:.2}",
            case.name,
            (times[(times.len() - 1) / 2] + times[times.len() / 2]) / 2.0,
            times[0],
            times[times.len() - 1]
        );
    }
    Ok(())
}
