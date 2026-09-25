use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_expr::projection::update_expr;
use datafusion::physical_expr::utils::{conjunction, split_conjunction};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use std::sync::Arc;

/// Evaluate dynamic predicates on decoded rows when Parquet only uses them for pruning.
pub(super) fn insert_post_scan_dynamic_filters(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    if !config.optimizer.enable_dynamic_filter_pushdown || config.execution.parquet.pushdown_filters
    {
        return Ok(plan);
    }
    plan.transform_up(|node| {
        let Some(scan) = node
            .downcast_ref::<DataSourceExec>()
            .and_then(|scan| scan.data_source().downcast_ref::<FileScanConfig>())
        else {
            return Ok(Transformed::no(node));
        };
        let Some(parquet) = scan.file_source().downcast_ref::<ParquetSource>() else {
            return Ok(Transformed::no(node));
        };
        // Filtering after an already-applied fetch could change LIMIT semantics.
        if scan.limit.is_some() || parquet.table_parquet_options().global.pushdown_filters {
            return Ok(Transformed::no(node));
        }
        let Some(predicate) = scan.file_source().filter() else {
            return Ok(Transformed::no(node));
        };
        let mut filters = vec![];
        for predicate in split_conjunction(&predicate) {
            if !predicate.is::<DynamicFilterPhysicalExpr>() {
                continue;
            }
            // Derived dynamic expressions keep the producer's shared state while remapping
            // its columns. Skip predicates whose inputs were projected out of the scan.
            let projected = match scan.file_source().projection() {
                Some(projection) => update_expr(predicate, projection.as_ref(), false)?,
                None => Some(Arc::clone(predicate)),
            };
            if let Some(projected) = projected {
                filters.push(projected);
            }
        }
        if filters.is_empty() {
            return Ok(Transformed::no(node));
        }
        Ok(Transformed::yes(
            Arc::new(FilterExec::try_new(conjunction(filters), node)?) as Arc<dyn ExecutionPlan>,
        ))
    })
    .map(|transformed| transformed.data)
}
