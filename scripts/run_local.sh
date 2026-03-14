#!/bin/bash
set -euo pipefail

COMPOSE="docker compose -f docker/docker-compose.yml"

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
    echo "=== SOME VERIFICATIONS FAILED ==="
    exit 1
fi

echo "=== Test complete! ==="
echo "To tear down: $COMPOSE down -v"
