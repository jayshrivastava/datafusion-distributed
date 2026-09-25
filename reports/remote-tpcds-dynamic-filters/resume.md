# Paused After SF10

SF10 is complete: 120 blocks, 600 measured executions, and 120 excluded
warmups. Read [the SF10 report](sf10-report.md) and
[local-versus-remote comparison](local-vs-remote.md).

SF100 is deliberately paused at the user's review checkpoint. Its first
A/control pass is saved: 40 blocks and 200 measured executions. No B
SF100 case or second A/control block has run. These results are provisional.
Both S3 datasets are already complete; do not regenerate either dataset.

The benchmark process and port-forward have exited. The 12-worker A
deployment remains installed, without an active benchmark query. Do not
tear down the deployment or foundation as part of this handoff.

## Resume SF100 After Review

```bash
TOOLS=/home/bits/datafusion-distributed-dev-tools
export AWS_PROFILE=account-admin-891007267895
export AWS_REGION=us-east-1
cd "$TOOLS"

aws sts get-caller-identity
node .data/remote-tpcds-dynamic-filters/execute.mjs --dataset tpcds/sf100
```

If credentials have expired, refresh before retrying:

```bash
aws sso login --profile "$AWS_PROFILE"
```

The dataset-scoped runner skips the completed SF100 A/control pass, then
deploys fresh workers for B, B, and A. That is 80 remaining blocks,
400 measured executions, and 80 warmups. It does not rerun or modify SF10
measurements. The first A pass predates this review pause; preserve that
timing gap in any repeatability claim.

The raw logs preserve an earlier SSO interruption and the deliberate
SF10-first scheduling handoff. Neither caused a failed timed query.
See [plan.md](plan.md), [status.md](status.md), and
[manifest.json](manifest.json) for the retained provenance.

After SF100 finishes, complete its filter-route audit, regenerate the
numeric reports, and write a separate SF100 interpretation. Do not
silently overwrite the SF10 handoff report with provisional SF100 claims.
