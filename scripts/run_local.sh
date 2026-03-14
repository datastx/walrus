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

echo "=== Waiting for CDC propagation ==="
sleep 45

echo "=== Test complete! ==="
echo "To tear down: $COMPOSE down -v"
