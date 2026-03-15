#!/bin/bash
set -euo pipefail

COMPOSE="docker compose -f deploy/docker-compose.yml"

echo "=== Starting infrastructure ==="
$COMPOSE up -d source-postgres
sleep 5

echo "=== Starting WAL Capture ==="
$COMPOSE up -d wal-capture
sleep 10

echo "=== Starting Iceberg Writer ==="
$COMPOSE up -d iceberg-writer

echo "=== Waiting for backfill to complete ==="
until $COMPOSE exec source-postgres psql -U replicator -d sourcedb -tAc \
    "SELECT COUNT(*) FROM _pgiceberg.table_state WHERE phase != 'streaming'" 2>/dev/null | grep -q '^0$'; do
    echo "  Backfill in progress..."
    sleep 5
done
echo "  Backfill complete!"

echo "=== Waiting for writer to process backfill files ==="
until $COMPOSE exec source-postgres psql -U replicator -d sourcedb -tAc \
    "SELECT COUNT(*) FROM _pgiceberg.file_queue WHERE status = 'pending'" 2>/dev/null | grep -q '^0$'; do
    echo "  Writer processing..."
    sleep 5
done
echo "  All files processed!"

echo "=== Generating CDC traffic ==="
$COMPOSE exec source-postgres psql -U replicator -d sourcedb -c "
    INSERT INTO users (name, email) VALUES ('new_user_1', 'new@example.com');
    UPDATE users SET name = 'updated_user' WHERE id = 1;
    DELETE FROM users WHERE id = 2;
"

echo "=== Corner case: DELETE→INSERT→DELETE→INSERT same PK ==="
$COMPOSE exec source-postgres psql -U replicator -d sourcedb -c "
    DELETE FROM users WHERE id = 5;
    INSERT INTO users (id, name, email) VALUES (5, 'phoenix_v1', 'v1@example.com');
    DELETE FROM users WHERE id = 5;
    INSERT INTO users (id, name, email) VALUES (5, 'phoenix_v2', 'v2@example.com');
"

echo "=== Corner case: same-transaction multi-op on same PK ==="
$COMPOSE exec source-postgres psql -U replicator -d sourcedb -c "
    BEGIN;
    UPDATE users SET name = 'interim' WHERE id = 10;
    DELETE FROM users WHERE id = 10;
    INSERT INTO users (id, name, email) VALUES (10, 'reborn', 'reborn@example.com');
    COMMIT;
"

echo "=== Corner case: rapid updates same row ==="
$COMPOSE exec source-postgres psql -U replicator -d sourcedb -c "
    UPDATE users SET name = 'v1' WHERE id = 20;
    UPDATE users SET name = 'v2' WHERE id = 20;
    UPDATE users SET name = 'v3' WHERE id = 20;
    UPDATE users SET name = 'v4' WHERE id = 20;
    UPDATE users SET name = 'v5_final' WHERE id = 20;
"

echo "=== Waiting for CDC propagation ==="
sleep 45

echo "=== Verifying corner case results ==="
FAIL=0

# Verify: id=5 should have name='phoenix_v2', exactly 1 row
COUNT_5=$($COMPOSE exec source-postgres psql -U replicator -d sourcedb -tAc \
    "SELECT COUNT(*) FROM users WHERE id = 5 AND name = 'phoenix_v2'")
if [ "$COUNT_5" != "1" ]; then
    echo "FAIL: id=5 expected 1 row with name='phoenix_v2', got count=$COUNT_5"
    FAIL=1
else
    echo "  PASS: id=5 has name='phoenix_v2'"
fi

# Verify: id=10 should have name='reborn', exactly 1 row
COUNT_10=$($COMPOSE exec source-postgres psql -U replicator -d sourcedb -tAc \
    "SELECT COUNT(*) FROM users WHERE id = 10 AND name = 'reborn'")
if [ "$COUNT_10" != "1" ]; then
    echo "FAIL: id=10 expected 1 row with name='reborn', got count=$COUNT_10"
    FAIL=1
else
    echo "  PASS: id=10 has name='reborn'"
fi

# Verify: id=20 should have name='v5_final', exactly 1 row
COUNT_20=$($COMPOSE exec source-postgres psql -U replicator -d sourcedb -tAc \
    "SELECT COUNT(*) FROM users WHERE id = 20 AND name = 'v5_final'")
if [ "$COUNT_20" != "1" ]; then
    echo "FAIL: id=20 expected 1 row with name='v5_final', got count=$COUNT_20"
    FAIL=1
else
    echo "  PASS: id=20 has name='v5_final'"
fi

if [ "$FAIL" -eq 1 ]; then
    echo "=== SOURCE VERIFICATIONS FAILED ==="
    exit 1
fi
echo "  All source verifications passed."

# ---------------------------------------------------------------------------
# Iceberg warehouse verification via DuckDB
# ---------------------------------------------------------------------------
echo ""
echo "=== Verifying Iceberg warehouse output ==="

echo "  Starting duckdb-cli service (debug profile)..."
$COMPOSE --profile debug up -d duckdb-cli

echo "  Waiting for DuckDB CLI to be installed..."
until $COMPOSE exec duckdb-cli test -x /usr/local/bin/duckdb 2>/dev/null; do
    echo "    Installing DuckDB..."
    sleep 5
done
echo "  DuckDB CLI ready."

# Helper: run a DuckDB query inside the duckdb-cli container and return output.
# Usage: duckdb_query "SQL"
duckdb_query() {
    $COMPOSE exec -T duckdb-cli duckdb -csv -noheader -c "$1" 2>/dev/null | tr -d '[:space:]'
}

# Helper: run a query that may return text with spaces; only trim leading/trailing whitespace.
duckdb_query_text() {
    $COMPOSE exec -T duckdb-cli duckdb -csv -noheader -c "$1" 2>/dev/null | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
}

echo ""
echo "--- Backfill row count verification ---"

# Verify users table row count (seed = 10000, +1 insert, -1 delete, net = 10000)
# The CDC operations are: INSERT new_user_1, UPDATE id=1, DELETE id=2,
# DELETE+INSERT+DELETE+INSERT id=5, DELETE+INSERT id=10, UPDATEs id=20.
# Net row count: 10000 (seed) + 1 (new_user_1) - 1 (DELETE id=2) = 10000
USERS_COUNT=$(duckdb_query "SELECT COUNT(*) FROM read_parquet('/data/iceberg/public/users/data/*.parquet');")
echo "  Iceberg users row count: $USERS_COUNT"
if [ -z "$USERS_COUNT" ] || [ "$USERS_COUNT" -lt 10000 ]; then
    echo "  FAIL: users expected >= 10000 rows, got ${USERS_COUNT:-<empty>}"
    FAIL=1
else
    echo "  PASS: users row count ($USERS_COUNT) >= 10000"
fi

# Verify orders table row count (seed = 50000, no CDC changes to orders)
ORDERS_COUNT=$(duckdb_query "SELECT COUNT(*) FROM read_parquet('/data/iceberg/public/orders/data/*.parquet');")
echo "  Iceberg orders row count: $ORDERS_COUNT"
if [ -z "$ORDERS_COUNT" ] || [ "$ORDERS_COUNT" -lt 50000 ]; then
    echo "  FAIL: orders expected >= 50000 rows, got ${ORDERS_COUNT:-<empty>}"
    FAIL=1
else
    echo "  PASS: orders row count ($ORDERS_COUNT) >= 50000"
fi

# Verify products table row count (seed = 5000, no CDC changes to products)
PRODUCTS_COUNT=$(duckdb_query "SELECT COUNT(*) FROM read_parquet('/data/iceberg/public/products/data/*.parquet');")
echo "  Iceberg products row count: $PRODUCTS_COUNT"
if [ -z "$PRODUCTS_COUNT" ] || [ "$PRODUCTS_COUNT" -lt 5000 ]; then
    echo "  FAIL: products expected >= 5000 rows, got ${PRODUCTS_COUNT:-<empty>}"
    FAIL=1
else
    echo "  PASS: products row count ($PRODUCTS_COUNT) >= 5000"
fi

echo ""
echo "--- CDC change verification in Iceberg ---"

# Verify: id=5 should have name='phoenix_v2' (DELETE/INSERT cycles)
ICE_NAME_5=$(duckdb_query_text "SELECT name FROM read_parquet('/data/iceberg/public/users/data/*.parquet') WHERE id = 5;")
if [ "$ICE_NAME_5" != "phoenix_v2" ]; then
    echo "  FAIL: Iceberg id=5 expected name='phoenix_v2', got '${ICE_NAME_5}'"
    FAIL=1
else
    echo "  PASS: Iceberg id=5 has name='phoenix_v2'"
fi

# Verify: id=10 should have name='reborn' (same-transaction multi-op)
ICE_NAME_10=$(duckdb_query_text "SELECT name FROM read_parquet('/data/iceberg/public/users/data/*.parquet') WHERE id = 10;")
if [ "$ICE_NAME_10" != "reborn" ]; then
    echo "  FAIL: Iceberg id=10 expected name='reborn', got '${ICE_NAME_10}'"
    FAIL=1
else
    echo "  PASS: Iceberg id=10 has name='reborn'"
fi

# Verify: id=20 should have name='v5_final' (rapid updates)
ICE_NAME_20=$(duckdb_query_text "SELECT name FROM read_parquet('/data/iceberg/public/users/data/*.parquet') WHERE id = 20;")
if [ "$ICE_NAME_20" != "v5_final" ]; then
    echo "  FAIL: Iceberg id=20 expected name='v5_final', got '${ICE_NAME_20}'"
    FAIL=1
else
    echo "  PASS: Iceberg id=20 has name='v5_final'"
fi

# Verify: id=2 should NOT be present (deleted row)
ICE_COUNT_2=$(duckdb_query "SELECT COUNT(*) FROM read_parquet('/data/iceberg/public/users/data/*.parquet') WHERE id = 2;")
if [ "$ICE_COUNT_2" != "0" ]; then
    echo "  FAIL: Iceberg id=2 should be deleted, but found $ICE_COUNT_2 row(s)"
    FAIL=1
else
    echo "  PASS: Iceberg id=2 correctly absent (deleted)"
fi

echo ""
if [ "$FAIL" -eq 1 ]; then
    echo "=== SOME VERIFICATIONS FAILED ==="
    exit 1
fi

echo "=== All verifications passed! ==="
echo "To tear down: $COMPOSE --profile debug down -v"
