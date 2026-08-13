#!/usr/bin/env bash
set -euo pipefail

BENCH_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BENCH_REPO_ROOT"

BENCH_COUNT="${BENCH_COUNT:-1000}"
BENCH_WORKERS="${BENCH_WORKERS:-64}"
BENCH_SCHEDULER_INTERVAL="${BENCH_SCHEDULER_INTERVAL:-1s}"
BENCH_LOAD_CONCURRENCY="${BENCH_LOAD_CONCURRENCY:-32}"
BENCH_LEAD_SECONDS="${BENCH_LEAD_SECONDS:-45}"
BENCH_WAIT_SECONDS="${BENCH_WAIT_SECONDS:-360}"
BENCH_RUN_ID="${BENCH_RUN_ID:-standalone-$(date --utc '+%Y%m%dT%H%M%SZ')}"
BENCH_ARTIFACT_DIR="${BENCH_ARTIFACT_DIR:-$BENCH_REPO_ROOT/benchmark-artifacts/$BENCH_RUN_ID}"
BENCH_COMPOSE_PROJECT="${BENCH_COMPOSE_PROJECT:-go-scheduler-benchmark-$$}"
BENCH_KEEP_STACK="${BENCH_KEEP_STACK:-false}"
BENCH_PG_STATS="${BENCH_PG_STATS:-false}"

for BENCH_BINARY in docker go curl jq sed date mktemp; do
  if ! command -v "$BENCH_BINARY" >/dev/null 2>&1; then
    echo "required command is unavailable: $BENCH_BINARY" >&2
    exit 1
  fi
done
if ! [[ "$BENCH_COUNT" =~ ^[1-9][0-9]*$ ]] || \
   ! [[ "$BENCH_WORKERS" =~ ^[1-9][0-9]*$ ]] || \
   ! [[ "$BENCH_LOAD_CONCURRENCY" =~ ^[1-9][0-9]*$ ]] || \
   ! [[ "$BENCH_LEAD_SECONDS" =~ ^[0-9]+$ ]] || \
   ! [[ "$BENCH_WAIT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "count, workers, concurrency, lead, and wait must be positive integers" >&2
  exit 1
fi
if (( BENCH_COUNT > 100000 || BENCH_LOAD_CONCURRENCY > 256 || BENCH_LEAD_SECONDS < 31 )); then
  echo "count must be <=100000, concurrency <=256, and lead >=31 seconds" >&2
  exit 1
fi
if [[ "$BENCH_KEEP_STACK" != true && "$BENCH_KEEP_STACK" != false ]] || [[ "$BENCH_PG_STATS" != true && "$BENCH_PG_STATS" != false ]]; then
  echo "BENCH_KEEP_STACK and BENCH_PG_STATS must be true or false" >&2
  exit 1
fi

mkdir -p "$BENCH_ARTIFACT_DIR"
BENCH_COMPOSE=(docker compose --project-name "$BENCH_COMPOSE_PROJECT" --profile executor --file deploy/docker-compose.yml --file deploy/docker-compose.benchmark.yml)
if [[ "$BENCH_PG_STATS" == true ]]; then
  BENCH_COMPOSE+=(--file deploy/docker-compose.benchmark-profile.yml)
fi
BENCH_CAPTURED=false
BENCH_TEMP_DIR="$(mktemp -d)"
BENCH_TOOL="$BENCH_TEMP_DIR/scheduler-bench"
BENCH_EXECUTOR_GROUP_ID=""
BENCH_EXECUTOR_TENANT_ID=""
export BENCH_EXECUTOR_GROUP_ID BENCH_EXECUTOR_TENANT_ID

benchmark_capture() {
  if [[ "$BENCH_CAPTURED" == true ]]; then
    return
  fi
  BENCH_CAPTURED=true
  "${BENCH_COMPOSE[@]}" exec -T postgres psql -U scheduler -d scheduler -X --csv -c \
    "SELECT status,count(*) FROM job_runs GROUP BY status ORDER BY status" \
    > "$BENCH_ARTIFACT_DIR/run-status.csv" 2>/dev/null || true
  "${BENCH_COMPOSE[@]}" exec -T postgres psql -U scheduler -d scheduler -X --csv -c \
    "SELECT count(*) AS runs,round(extract(epoch FROM max(finished_at)-min(scheduled_at))::numeric,3) AS window_seconds,round(percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM started_at-scheduled_at))::numeric,6) AS dispatch_p50_seconds,round(percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM started_at-scheduled_at))::numeric,6) AS dispatch_p99_seconds,max(started_at-scheduled_at) AS dispatch_max FROM job_runs WHERE trigger_type='schedule'" \
    > "$BENCH_ARTIFACT_DIR/database-summary.csv" 2>/dev/null || true
  "${BENCH_COMPOSE[@]}" exec -T postgres psql -U scheduler -d scheduler -X --csv -c \
    "SELECT calls,round(total_exec_time::numeric,3) AS total_exec_ms,round(mean_exec_time::numeric,3) AS mean_exec_ms,rows,left(regexp_replace(query,E'[\\n\\r\\t ]+',' ','g'),500) AS query FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 30" \
    > "$BENCH_ARTIFACT_DIR/pg-stat-statements.csv" 2>/dev/null || true
  "${BENCH_COMPOSE[@]}" logs --no-color > "$BENCH_ARTIFACT_DIR/compose.log" 2>&1 || true
}

benchmark_cleanup() {
  BENCH_EXIT_CODE=$?
  benchmark_capture
  if [[ "$BENCH_KEEP_STACK" == true ]]; then
    echo "benchmark stack retained: $BENCH_COMPOSE_PROJECT" >&2
  else
    "${BENCH_COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf -- "$BENCH_TEMP_DIR"
  exit "$BENCH_EXIT_CODE"
}
trap benchmark_cleanup EXIT

{
  uname -a
  command -v lscpu >/dev/null 2>&1 && lscpu || true
  command -v free >/dev/null 2>&1 && free -h || true
  docker version
  printf 'commit=%s\ncount=%s\nworkers=%s\nscheduler_interval=%s\nload_concurrency=%s\n' \
    "$(git rev-parse HEAD 2>/dev/null || printf unknown)" "$BENCH_COUNT" "$BENCH_WORKERS" \
    "$BENCH_SCHEDULER_INTERVAL" "$BENCH_LOAD_CONCURRENCY"
} > "$BENCH_ARTIFACT_DIR/environment.txt"

echo "starting isolated single-node benchmark stack" >&2
BENCH_WORKERS="$BENCH_WORKERS" BENCH_SCHEDULER_INTERVAL="$BENCH_SCHEDULER_INTERVAL" \
  "${BENCH_COMPOSE[@]}" build --quiet benchmark-sink scheduler-server executor migrate bootstrap postgres
BENCH_WORKERS="$BENCH_WORKERS" BENCH_SCHEDULER_INTERVAL="$BENCH_SCHEDULER_INTERVAL" \
  "${BENCH_COMPOSE[@]}" up --no-build --detach benchmark-sink scheduler-server

for BENCH_ATTEMPT in $(seq 1 90); do
  if curl --fail --silent http://127.0.0.1:18080/health/ready >/dev/null && \
     curl --fail --silent http://127.0.0.1:19090/health >/dev/null; then
    break
  fi
  if [[ "$BENCH_ATTEMPT" == 90 ]]; then
    echo "benchmark services did not become healthy" >&2
    exit 1
  fi
  sleep 1
done

if [[ "$BENCH_PG_STATS" == true ]]; then
  "${BENCH_COMPOSE[@]}" exec -T postgres psql -U scheduler -d scheduler -X -c \
    "CREATE EXTENSION IF NOT EXISTS pg_stat_statements; SELECT pg_stat_statements_reset();" >/dev/null
fi

BENCH_BOOTSTRAP_LOG="$("${BENCH_COMPOSE[@]}" logs --no-color bootstrap)"
BENCH_TOKEN="$(sed -n 's/.*api_key=\(gsk_[^[:space:]]*\).*/\1/p' <<<"$BENCH_BOOTSTRAP_LOG" | tail -1)"
BENCH_TENANT_ID="$(sed -n 's/.*tenant_id=\([^[:space:]]*\).*/\1/p' <<<"$BENCH_BOOTSTRAP_LOG" | tail -1)"
if [[ -z "$BENCH_TOKEN" || -z "$BENCH_TENANT_ID" ]]; then
  echo "bootstrap credentials were not found" >&2
  exit 1
fi
export BENCH_TOKEN BENCH_TENANT_ID

BENCH_GROUP_RESPONSE="$(curl --fail --silent --show-error \
  --request POST http://127.0.0.1:18080/api/v1/executor-groups \
  --header "Authorization: Bearer $BENCH_TOKEN" \
  --header "X-Tenant-ID: $BENCH_TENANT_ID" \
  --header 'Content-Type: application/json' \
  --data '{"name":"standalone-benchmark","route_strategy":"round","registration_mode":"automatic"}')"
BENCH_EXECUTOR_GROUP_ID="$(jq -r '.id // empty' <<<"$BENCH_GROUP_RESPONSE")"
BENCH_EXECUTOR_TENANT_ID="$BENCH_TENANT_ID"
export BENCH_EXECUTOR_GROUP_ID BENCH_EXECUTOR_TENANT_ID
if [[ -z "$BENCH_EXECUTOR_GROUP_ID" ]]; then
  echo "benchmark executor group was not created" >&2
  exit 1
fi

"${BENCH_COMPOSE[@]}" up --no-build --no-deps --detach executor
for BENCH_ATTEMPT in $(seq 1 60); do
  BENCH_REGISTERED="$("${BENCH_COMPOSE[@]}" exec -T postgres psql -U scheduler -d scheduler -X --tuples-only --no-align -c \
    "SELECT count(*) FROM executor_nodes WHERE group_id='$BENCH_EXECUTOR_GROUP_ID'::uuid AND expires_at>now()" 2>/dev/null || true)"
  if [[ "$BENCH_REGISTERED" == 1 ]]; then
    break
  fi
  if [[ "$BENCH_ATTEMPT" == 60 ]]; then
    echo "benchmark executor did not register" >&2
    exit 1
  fi
  sleep 1
done

go build -trimpath -o "$BENCH_TOOL" ./cmd/scheduler-bench
BENCH_SCHEDULED_AT="$(date --utc --date="+$BENCH_LEAD_SECONDS seconds" '+%Y-%m-%dT%H:%M:%SZ')"
echo "loading $BENCH_COUNT jobs scheduled at $BENCH_SCHEDULED_AT" >&2
"$BENCH_TOOL" load \
  --system go \
  --server http://127.0.0.1:18080 \
  --sink http://benchmark-sink:19090/execute \
  --sink-control http://127.0.0.1:19090/execute \
  --run-id "$BENCH_RUN_ID" \
  --count "$BENCH_COUNT" \
  --concurrency "$BENCH_LOAD_CONCURRENCY" \
  --scheduled-at "$BENCH_SCHEDULED_AT" \
  --go-executor-group "$BENCH_EXECUTOR_GROUP_ID" \
  > "$BENCH_ARTIFACT_DIR/manifest.json"

if [[ "$BENCH_PG_STATS" == true ]]; then
  "${BENCH_COMPOSE[@]}" exec -T postgres psql -U scheduler -d scheduler -X -c \
    "SELECT pg_stat_statements_reset();" >/dev/null
fi

while [[ "$(date +%s)" -lt "$(date --date="$BENCH_SCHEDULED_AT" +%s)" ]]; do
  sleep 1
done

BENCH_COMPLETED=false
for BENCH_SAMPLE in $(seq 1 "$BENCH_WAIT_SECONDS"); do
  BENCH_TIMESTAMP="$(date --utc '+%Y-%m-%dT%H:%M:%S.%3NZ')"
  BENCH_REPORT="$(curl --fail --silent http://127.0.0.1:19090/api/v1/report)"
  jq --compact-output --arg timestamp "$BENCH_TIMESTAMP" --argjson sample "$BENCH_SAMPLE" \
    '. + {sampled_at: $timestamp, sample: $sample}' <<<"$BENCH_REPORT" \
    >> "$BENCH_ARTIFACT_DIR/report-series.jsonl"
  curl --fail --silent http://127.0.0.1:18080/metrics > "$BENCH_ARTIFACT_DIR/metrics-latest.prom"
  "${BENCH_COMPOSE[@]}" stats --no-stream --format '{{json .}}' \
    | jq --compact-output --arg timestamp "$BENCH_TIMESTAMP" '. + {sampled_at: $timestamp}' \
    >> "$BENCH_ARTIFACT_DIR/docker-stats.jsonl"
  if [[ "$(jq -r '.missing' <<<"$BENCH_REPORT")" == 0 ]]; then
    jq . <<<"$BENCH_REPORT" > "$BENCH_ARTIFACT_DIR/report.json"
    BENCH_COMPLETED=true
    break
  fi
  sleep 1
done

if [[ "$BENCH_COMPLETED" != true ]]; then
  echo "benchmark did not complete within $BENCH_WAIT_SECONDS seconds" >&2
  exit 1
fi
jq -e '.missing == 0 and .duplicate_requests == 0 and .unexpected_requests == 0 and .invalid_requests == 0' \
  "$BENCH_ARTIFACT_DIR/report.json" >/dev/null
benchmark_capture

echo "benchmark completed; result: $BENCH_ARTIFACT_DIR/report.json" >&2
jq . "$BENCH_ARTIFACT_DIR/report.json"
