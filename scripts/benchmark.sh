#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
rows="${ROWS:-10000}"
if [[ ! "$rows" =~ ^[1-9][0-9]{0,7}$ ]]; then
  echo 'ROWS must be an integer from 1 through 99999999' >&2
  exit 1
fi
export SOURCE_URI='postgres://dbmix:dbmix@127.0.0.1:55432/dbmix?sslmode=disable'
export DESTINATION_URI='dbmix:dbmix@tcp(127.0.0.1:53306)/dbmix'
mkdir -p .dbmix
chmod 700 .dbmix
docker compose exec -T postgres psql -U dbmix -d dbmix -v ON_ERROR_STOP=1 -v rows="$rows" <<'SQL'
CREATE TABLE IF NOT EXISTS benchmark_source(id BIGINT PRIMARY KEY,email TEXT NOT NULL,metadata JSONB NOT NULL);
TRUNCATE benchmark_source;
INSERT INTO benchmark_source
SELECT n, 'user-' || n || '@example.test', jsonb_build_object('active',true,'visits',n)
FROM generate_series(1, :rows) n;
ANALYZE benchmark_source;
SQL
# Clear only the benchmark fixture so repeated runs measure initial loads.
docker compose exec -T mysql mysql -udbmix -pdbmix dbmix -e 'DROP TABLE IF EXISTS benchmark_target'
cat > .dbmix/benchmark.yaml <<'YAML'
version: 1
source: {type: postgres, uri: '${SOURCE_URI}', allow_insecure: true}
destination: {type: mysql, uri: '${DESTINATION_URI}', allow_insecure: true}
source_quiescent: true
batch_size: 1000
workers: 4
queue_size: 4
entities:
  - {source: benchmark_source, target: benchmark_target, key: id}
YAML
./bin/dbmix migrate -f .dbmix/benchmark.yaml 2>.dbmix/benchmark-log.jsonl | tee .dbmix/benchmark-result.json
docker compose exec -T mysql mysql -udbmix -pdbmix dbmix -e 'SELECT COUNT(*) AS records,SUM(id) AS id_sum FROM benchmark_target'
