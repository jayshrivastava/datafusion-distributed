use crate::common::OnceLockResult;
use crate::common::now_ns;
use crate::{
    FirstLatencyMetric, LatencyMetricExt, MaxLatencyMetric, ProducerHead,
    TaskCompletedDynamicFilters, TaskMetrics,
};
use datafusion::common::instant::Instant;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::poll_fn;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
/// TaskData stores state for a single task being executed by this Endpoint. It may be shared
/// by concurrent requests for the same task which execute separate partitions.
pub struct TaskData {
    /// Task context suitable for execute different partitions from the same task.
    pub(crate) task_ctx: Arc<TaskContext>,
    pub(crate) base_plan: Arc<dyn ExecutionPlan>,
    pub(crate) final_plan: Arc<OnceLockResult<Arc<dyn ExecutionPlan>>>,
    /// Sender half of the metrics channel. `impl_coordinator_channel` takes this (via
    /// `Option::take`) when the coordinator channel reaches EOS, sending the collected metrics
    /// back to the coordinator through the `CoordinatorChannel` side channel.
    pub(super) metrics_tx: Arc<std::sync::Mutex<Option<oneshot::Sender<TaskMetrics>>>>,
    /// Sender half of the completed dynamic-filter channel. It is absent when the user does not
    /// want to display dynamic filters.
    pub(super) completed_dynamic_filters_tx:
        Arc<std::sync::Mutex<Option<oneshot::Sender<TaskCompletedDynamicFilters>>>>,
    /// Metrics related to the execution of a task within a stage. This metrics, instead of being
    /// associated to a specific node, they are global to the task, like the time at which the plan
    /// was fed by the coordinator to the worker.
    pub(super) task_data_metrics: Arc<TaskDataMetrics>,
}

pub(crate) const PLAN_ADDED_AT_METRIC: &str = "plan_added_at";
pub(crate) const PLAN_EXECUTED_AT_METRIC: &str = "plan_executed_at";
pub(crate) const PLAN_FINISHED_AT_METRIC: &str = "plan_finished_at";

#[derive(Debug)]
pub(super) struct TaskDataMetrics {
    pub(super) query_start_time_ns: usize,
    enabled: bool,
    metrics: ExecutionPlanMetricsSet,
    /// When the plan execution was triggered by the parent worker.
    pub(super) plan_executed_at: MaxLatencyMetric,
    /// When the coordinator channel closed and final reporting began, not stream EOS.
    pub(super) plan_finished_at: MaxLatencyMetric,
    output_first_batch_at: FirstLatencyMetric,
    output_last_batch_at: MaxLatencyMetric,
    output_stream_finished_at: MaxLatencyMetric,
    output_streams_started: Count,
    output_streams_completed: Count,
    output_poll_time: Time,
    output_poll_max: MaxLatencyMetric,
    dynamic_filter_first_applied_at: FirstLatencyMetric,
    dynamic_filter_last_applied_at: MaxLatencyMetric,
    dynamic_filter_updates_applied: Count,
    dynamic_filter_apply_time: Time,
}

impl TaskDataMetrics {
    pub(super) fn new(query_start_time_ns: usize, enabled: bool) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        let plan_added_at = MetricBuilder::new(&metrics).max_latency(PLAN_ADDED_AT_METRIC);
        plan_added_at.add_duration(Duration::from_nanos(
            now_ns::<u64>().saturating_sub(query_start_time_ns as u64),
        ));
        Self {
            query_start_time_ns,
            enabled,
            plan_finished_at: MetricBuilder::new(&metrics).max_latency(PLAN_FINISHED_AT_METRIC),
            plan_executed_at: MetricBuilder::new(&metrics).max_latency(PLAN_EXECUTED_AT_METRIC),
            output_first_batch_at: MetricBuilder::new(&metrics)
                .first_latency("output_first_batch_at"),
            output_last_batch_at: MetricBuilder::new(&metrics).max_latency("output_last_batch_at"),
            output_stream_finished_at: MetricBuilder::new(&metrics)
                .max_latency("output_stream_finished_at"),
            output_streams_started: MetricBuilder::new(&metrics)
                .global_counter("output_streams_started"),
            output_streams_completed: MetricBuilder::new(&metrics)
                .global_counter("output_streams_completed"),
            output_poll_time: MetricBuilder::new(&metrics).subset_time("output_poll_time", 0),
            output_poll_max: MetricBuilder::new(&metrics).max_latency("output_poll_max"),
            dynamic_filter_first_applied_at: MetricBuilder::new(&metrics)
                .first_latency("dynamic_filter_first_applied_at"),
            dynamic_filter_last_applied_at: MetricBuilder::new(&metrics)
                .max_latency("dynamic_filter_last_applied_at"),
            dynamic_filter_updates_applied: MetricBuilder::new(&metrics)
                .global_counter("dynamic_filter_updates_applied"),
            dynamic_filter_apply_time: MetricBuilder::new(&metrics)
                .subset_time("dynamic_filter_apply_time", 0),
            metrics,
        }
    }

    fn query_elapsed(&self) -> Duration {
        Duration::from_nanos(now_ns::<u64>().saturating_sub(self.query_start_time_ns as u64))
    }

    pub(super) fn mark_execution_started_once(&self) {
        if self.plan_executed_at.value() == 0 {
            self.plan_executed_at.add_duration(Duration::from_nanos(
                now_ns::<u64>().saturating_sub(self.query_start_time_ns as u64),
            ))
        }
    }

    pub(super) fn mark_execution_finished(&self) {
        self.plan_finished_at.add_duration(Duration::from_nanos(
            now_ns::<u64>().saturating_sub(self.query_start_time_ns as u64),
        ))
    }

    pub(super) fn to_metrics_set(&self) -> MetricsSet {
        self.metrics.clone_inner()
    }

    pub(super) fn record_dynamic_filter_applied(&self, start: Instant) {
        if !self.enabled {
            return;
        }
        self.dynamic_filter_updates_applied.add(1);
        self.dynamic_filter_apply_time.add_elapsed(start);
        let elapsed = self.query_elapsed();
        self.dynamic_filter_first_applied_at.add_duration(elapsed);
        self.dynamic_filter_last_applied_at.add_duration(elapsed);
    }

    pub(super) fn track_stream(
        self: &Arc<Self>,
        mut stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        if !self.enabled {
            return stream;
        }
        let schema = stream.schema();
        let metrics = Arc::clone(self);
        let mut completed = false;
        metrics.output_streams_started.add(1);
        let stream = poll_fn(move |cx| {
            if completed {
                return Poll::Ready(None);
            }
            // Poll time is inclusive wall time, not CPU time or time awaiting a wakeup.
            let start = Instant::now();
            let result = stream.as_mut().poll_next(cx);
            let duration = start.elapsed();
            metrics.output_poll_time.add_duration(duration);
            metrics.output_poll_max.add_duration(duration);
            match &result {
                Poll::Ready(Some(Ok(_))) => {
                    let elapsed = metrics.query_elapsed();
                    metrics.output_first_batch_at.add_duration(elapsed);
                    metrics.output_last_batch_at.add_duration(elapsed);
                }
                Poll::Ready(None) => {
                    completed = true;
                    metrics.output_streams_completed.add(1);
                    metrics
                        .output_stream_finished_at
                        .add_duration(metrics.query_elapsed());
                }
                _ => {}
            }
            result
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

impl TaskData {
    pub(crate) fn plan(&self, producer_head: ProducerHead) -> Result<Arc<dyn ExecutionPlan>> {
        let result = self.final_plan.get_or_init(|| {
            let producer_head =
                producer_head.ensure_decoded(self.base_plan.schema(), &self.task_ctx)?;

            Ok(producer_head.insert(Arc::clone(&self.base_plan))?)
        });
        match result {
            Ok(plan) => Ok(Arc::clone(plan)),
            Err(err) => Err(DataFusionError::Shared(Arc::clone(err))),
        }
    }
}
