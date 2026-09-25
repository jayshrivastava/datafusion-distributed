#[cfg(test)]
mod tests {
    use crate::common::TestQuery;
    use datafusion::common::Result;
    use test_case::test_case;

    #[tokio::test]
    async fn displays_intermediate_dynamic_filter() -> Result<()> {
        let display = TestQuery::new(
            r#"
                WITH totals AS (
                    SELECT "RainToday", SUM(CAST("MinTemp" AS BIGINT)) AS total
                    FROM weather GROUP BY "RainToday"
                )
                SELECT names.key, totals.total
                FROM (SELECT DISTINCT "RainToday" AS key FROM weather) names
                JOIN totals ON names.key = totals."RainToday"
                WHERE total = (SELECT MAX(total) FROM totals)
                ORDER BY names.key
            "#,
        )
        .with_broadcast_joins()
        // Disable the post-scan fallback so only the existing intermediate filter can match.
        .with_parquet_pushdown(true)
        .execute()
        .await?;
        assert!(
            display.contains(
                "FilterExec: task_variants={t0: [FilterExec: DynamicFilter [ expression_id_"
            ),
            "{display}"
        );
        Ok(())
    }

    /// RainToday is column 19 in the file, but column 0 (aliased to key) in the scan output.
    #[test_case(false, false; "static_post_scan")]
    #[test_case(false, true; "adaptive_post_scan")]
    #[test_case(true, false; "parquet_pushdown_needs_no_fallback")]
    #[tokio::test]
    async fn post_scan_filter_remaps_projected_columns(
        pushdown: bool,
        adaptive: bool,
    ) -> Result<()> {
        let mut query = TestQuery::new(
            r#"
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT "RainToday" AS key FROM weather
                ) build
                RIGHT SEMI JOIN (
                    SELECT DISTINCT "RainToday" AS key FROM weather
                ) probe ON build.key = probe.key
            "#,
        )
        .with_broadcast_joins()
        .with_parquet_pushdown(pushdown);
        if adaptive {
            query = query.with_dynamic_task_count();
        } else {
            query = query.expect_dynamic_filter_updates();
        }
        let display = query.execute().await?;
        assert_eq!(
            display.contains("FilterExec: task_variants="),
            !pushdown,
            "{display}"
        );
        assert!(
            display.contains("projection=[RainToday@19 as key]"),
            "{display}"
        );
        Ok(())
    }
}
