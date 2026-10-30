#!/usr/bin/env bash
# End-to-end data-quality gate, used by CI and runnable locally:
#   1. initialise the schema
#   2. load sample batch 1 (initial load)     -> critical checks must pass
#   3. rerun with no changes                  -> must process 0 rows (incremental + idempotent)
#   4. load sample batch 2 (two weeks later)  -> critical checks must pass
#   5. run the analytics queries               -> every query must execute
# Any failure exits non-zero, which fails the CI job.
set -euo pipefail

echo "::group::init-db"
sales-pipeline init-db
echo "::endgroup::"

echo "::group::initial load (batch 1)"
sales-pipeline generate-data --batch 1
sales-pipeline run --start-mock-api
sales-pipeline quality
echo "::endgroup::"

echo "::group::rerun with unchanged sources (expect 0 incremental rows)"
output=$(sales-pipeline run --start-mock-api 2>&1)
echo "$output"
if ! grep -q "'incremental': 0" <<<"$output"; then
  echo "ERROR: rerun without source changes processed rows; incremental logic is broken" >&2
  exit 1
fi
echo "::endgroup::"

echo "::group::incremental load (batch 2)"
sales-pipeline generate-data --batch 2
sales-pipeline run --start-mock-api
sales-pipeline quality
echo "::endgroup::"

echo "::group::analytics queries"
sales-pipeline analytics
echo "::endgroup::"

echo "Data-quality gate passed."
