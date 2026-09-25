#[cfg(test)]
mod tests {
    use crate::common::TestQuery;
    use datafusion::common::Result;
    use datafusion_distributed::assert_snapshot;

    /// A TopK SortExec applies dynamic filters to local data sources.
    #[tokio::test]
    async fn local_dynamic_filters() -> Result<()> {
        let display = TestQuery::new(
            r#"
                SELECT "MinTemp"
                FROM weather
                ORDER BY "MinTemp" DESC
                LIMIT 10
            "#,
        )
        .with_expected_rows(10)
        .execute()
        .await?;

        // Each task in stage 1 produces a distinct filter to push down.
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [MinTemp@0 DESC], fetch=10
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=2, partitions=6
          │ SortExec: TopK(fetch=10), expr=[MinTemp@0 DESC], preserve_partitioning=[true]
          │   FilterExec: task_variants={t0: [FilterExec: DynamicFilter [ expression_id_0_hash_0 ]], t1: [FilterExec: DynamicFilter [ expression_id_0_hash_1 ]]}
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
          │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_1 ], dynamic_rg_pruning=eligible
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    /// A TopK sort updates dynamic filters on remote consumers across a shuffle.
    #[tokio::test]
    async fn remote_dynamic_filters() -> Result<()> {
        let display = TestQuery::new(
            r#"
                SELECT "MinTemp"
                FROM (
                    SELECT DISTINCT "MinTemp"
                    FROM weather
                )
                ORDER BY "MinTemp" DESC
                LIMIT 10
            "#,
        )
        .with_expected_rows(10)
        .expect_dynamic_filter_updates()
        .execute()
        .await?;

        // The per-task TopK filters in stage 2 are merged into one and passed to both consumers.
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [MinTemp@0 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=3
          │ SortExec: TopK(fetch=10), expr=[MinTemp@0 DESC], preserve_partitioning=[true]
          │   AggregateExec: mode=FinalPartitioned, gby=[MinTemp@0 as MinTemp], aggr=[], lim=[10]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([MinTemp@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[MinTemp@0 as MinTemp], aggr=[], lim=[10]
            │     FilterExec: task_variants={t0: [FilterExec: DynamicFilter [ expression_id_0_hash_0 ]], t1: [FilterExec: DynamicFilter [ expression_id_0_hash_0 ]]}
            │       DistributedLeafExec:
            │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
            │         t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }
}
