use crate::codec::encode_physical_expr;
use crate::{DistributedTaskContext, NetworkBoundary, NetworkShuffleExec, Stage, TaskKey};
use datafusion::common::{HashMap, Result, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashExpr, HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use datafusion_proto::protobuf::{
    PhysicalBinaryExprNode, PhysicalCaseNode, PhysicalDynamicFilterNode, PhysicalExprNode,
    PhysicalWhenThen,
};
use std::sync::Arc;

/// The task-local partitions are a contiguous slice of one global hash shuffle.
pub(super) struct HashRouting {
    context: DistributedTaskContext,
    partitions: usize,
    hash: PhysicalExprNode,
}

impl HashRouting {
    pub(super) fn for_join(
        join: &HashJoinExec,
        context: DistributedTaskContext,
        task_ctx: &TaskContext,
    ) -> Result<Option<Self>> {
        let keys: Vec<_> = join.on().iter().map(|(_, key)| Arc::clone(key)).collect();
        let build_keys: Vec<_> = join.on().iter().map(|(key, _)| Arc::clone(key)).collect();
        let partitions = join.right().output_partitioning().partition_count();
        if *join.partition_mode() != PartitionMode::Partitioned
            || context.task_count <= 1
            || !(has_shuffle_routing(join.left(), &build_keys, partitions)
                || has_shuffle_routing(join.right(), &keys, partitions))
        {
            return Ok(None);
        }
        let hash: Arc<dyn PhysicalExpr> = Arc::new(HashExpr::new(
            keys,
            REPARTITION_RANDOM_STATE.clone(),
            "hash_repartition".to_string(),
        ));
        Ok(Some(Self {
            context,
            partitions,
            hash: encode_physical_expr(&hash, task_ctx)?,
        }))
    }
}

// Deliberately conservative: hash partitioning modulo P alone does not prove which
// task owns a key modulo P*T. Trace unchanged keys to an actual remote shuffle.
fn has_shuffle_routing(
    plan: &Arc<dyn ExecutionPlan>,
    keys: &[Arc<dyn PhysicalExpr>],
    partitions: usize,
) -> bool {
    if !matches!(plan.output_partitioning(), Partitioning::Hash(actual, n)
        if *n == partitions && actual == keys)
    {
        return false;
    }
    if let Some(shuffle) = plan.downcast_ref::<NetworkShuffleExec>() {
        return matches!(shuffle.input_stage(), Stage::Remote(_));
    }
    let Some(indices) = keys
        .iter()
        .map(|key| key.downcast_ref::<Column>().map(Column::index))
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    let (input, input_keys) = if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let keys = indices
            .iter()
            .map(|&i| projection.expr().get(i).map(|expr| Arc::clone(&expr.expr)))
            .collect::<Option<Vec<_>>>();
        (projection.input(), keys)
    } else if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        let keys = indices
            .iter()
            .map(|&i| {
                let i = match filter.projection() {
                    Some(projection) => *projection.get(i)?,
                    None => i,
                };
                let schema = filter.input().schema();
                let field = schema.fields().get(i)?;
                Some(Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>)
            })
            .collect::<Option<Vec<_>>>();
        (filter.input(), keys)
    } else if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        if aggregate.group_expr().has_grouping_set() {
            return false;
        }
        let keys = indices
            .iter()
            .map(|&i| {
                aggregate
                    .group_expr()
                    .expr()
                    .get(i)
                    .map(|(expr, _)| Arc::clone(expr))
            })
            .collect::<Option<Vec<_>>>();
        (aggregate.input(), keys)
    } else {
        // In particular, do not infer task ownership through a local repartition,
        // range partitioning, a union, or an arbitrary custom operator.
        return false;
    };
    input_keys.is_some_and(|keys| has_shuffle_routing(input, &keys, partitions))
}

/// Replace the OR of local hash CASEs with one globally routed CASE. Return None
/// unless every task covers exactly one slice of the same hash space.
pub(super) fn merge_hash_predicates(
    reports: &[(&TaskKey, &PhysicalDynamicFilterNode)],
    routing: &HashMap<TaskKey, HashRouting>,
) -> Option<PhysicalExprNode> {
    let (first_task, _) = *reports.first()?;
    let first = routing.get(first_task)?;
    let task_count = first.context.task_count;
    let partitions = first.partitions;
    if reports.len() != task_count {
        return None;
    }
    let mut by_task = vec![None; task_count];
    for &(task, report) in reports {
        let route = routing.get(task)?;
        if task.stage_id != first_task.stage_id
            || route.context.task_count != task_count
            || route.partitions != partitions
            || route.hash != first.hash
        {
            return None;
        }
        if by_task
            .get_mut(route.context.task_index)?
            .replace(report)
            .is_some()
        {
            return None;
        }
    }
    let local_modulo = modulo(first.hash.clone(), partitions)?;
    let global_modulo = modulo(first.hash.clone(), partitions.checked_mul(task_count)?)?;
    let mut branches = Vec::with_capacity(partitions.checked_mul(task_count)?);
    for (task_index, report) in by_task.into_iter().enumerate() {
        let predicate = report?.inner_expr.as_deref()?;
        for partition in 0..partitions {
            let when = uint64(partition)?;
            let then = match &predicate.expr_type {
                Some(ExprType::Case(case)) if case.expr.as_deref() == Some(&local_modulo) => case
                    .when_then_expr
                    .iter()
                    .find(|branch| branch.when_expr.as_ref() == Some(&when))
                    .and_then(|branch| branch.then_expr.as_ref())
                    .or(case.else_expr.as_deref())?,
                // DataFusion may simplify a task with zero or one nonempty partition.
                // It can also wrap CASE in a null-preserving expression. Keep those
                // predicates intact, but evaluate them only for their owning task.
                _ => predicate,
            };
            branches.push(PhysicalWhenThen {
                when_expr: Some(uint64(task_index * partitions + partition)?),
                then_expr: Some(then.clone()),
            });
        }
    }
    Some(PhysicalExprNode {
        expr_id: None,
        expr_type: Some(ExprType::Case(Box::new(PhysicalCaseNode {
            expr: Some(Box::new(global_modulo)),
            when_then_expr: branches,
            // All hash buckets are present. Keep an unexpected/null route permissive.
            else_expr: Some(Box::new(PhysicalExprNode {
                expr_id: None,
                expr_type: Some(ExprType::Literal(
                    (&ScalarValue::Boolean(Some(true))).try_into().ok()?,
                )),
            })),
        }))),
    })
}

fn uint64(value: usize) -> Option<PhysicalExprNode> {
    Some(PhysicalExprNode {
        expr_id: None,
        expr_type: Some(ExprType::Literal(
            (&ScalarValue::UInt64(Some(u64::try_from(value).ok()?)))
                .try_into()
                .ok()?,
        )),
    })
}

fn modulo(hash: PhysicalExprNode, partitions: usize) -> Option<PhysicalExprNode> {
    Some(PhysicalExprNode {
        expr_id: None,
        expr_type: Some(ExprType::BinaryExpr(Box::new(PhysicalBinaryExprNode {
            l: None,
            r: None,
            op: "Modulo".to_string(),
            operands: vec![hash, uint64(partitions)?],
        }))),
    })
}
