#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# These credentials belong only to this repository's disposable local containers.
export SOURCE_URI='postgres://dbmix:dbmix@127.0.0.1:55432/dbmix?sslmode=disable'
export DESTINATION_URI='dbmix:dbmix@tcp(127.0.0.1:53306)/dbmix'
export MONGODB_URI='mongodb://127.0.0.1:57017'
docker compose exec -T postgres psql -U dbmix -d dbmix -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE IF NOT EXISTS customers (
  id BIGINT PRIMARY KEY,
  email TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  metadata JSONB NOT NULL
);
INSERT INTO customers
SELECT n, 'customer-' || n || '@example.test',
       '2024-01-01T12:00:00.123Z'::timestamptz,
       jsonb_build_object('tier', 'free', 'visits', n)
FROM generate_series(1,1000) n
ON CONFLICT(id) DO UPDATE SET email=EXCLUDED.email,
  created_at=EXCLUDED.created_at, metadata=EXCLUDED.metadata;
ANALYZE customers;
SQL
./bin/dbmix plan -f examples/postgres-mysql.yaml
./bin/dbmix migrate -f examples/postgres-mysql.yaml
./bin/dbmix migrate -f examples/postgres-mongodb.yaml
./bin/dbmix migrate -f examples/mongodb-postgres.yaml
./bin/dbmix migrate -f examples/mysql-postgres.yaml
docker compose exec -T mysql mysql -udbmix -pdbmix dbmix -e 'SELECT COUNT(*) AS migrated_customers FROM customers'
