#!/usr/bin/env bash
# Backfill polygon_markets.volume from Gamma API: first write condition_id,slug,volume to CSV, then update DB from CSV.
# Parallel fetch, progress, safe to run in background.
#
# Usage:
#   DB_HOST=... DB_USER=postgres DB_NAME=tradeflux PGPASSWORD=... ./scripts/backfill_market_volume.sh
#   # Or two steps: 1) only fetch to CSV (no DB update): FETCH_ONLY=1 ...; 2) load from CSV: LOAD_CSV=backfill_volume.csv ...
#
# Output: backfill_volume.csv (condition_id|slug|volume). Then DB is updated from it.
# Optional: WORKERS=20 GAMMA_URL=... LIMIT=1000 OFFSET=0 OUT_CSV=backfill_volume.csv
# Continue: use OFFSET for next batch (e.g. OFFSET=10000 LIMIT=10000), or just re-run (only volume IS NULL are selected).

set -e
GAMMA_URL="${GAMMA_URL:-https://gamma-api.polymarket.com}"
WORKERS="${WORKERS:-10}"
LIMIT="${LIMIT:-}"
OFFSET="${OFFSET:-0}"
OUT_CSV="${OUT_CSV:-backfill_volume.csv}"
FETCH_ONLY="${FETCH_ONLY:-}"
LOAD_CSV="${LOAD_CSV:-}"

export PGHOST="${DB_HOST:-}"
export PGUSER="${DB_USER:-postgres}"
export PGDATABASE="${DB_NAME:-tradeflux}"
export PGPASSWORD="${DB_PASSWORD:-}"

# --- Phase 2: Load from CSV and update DB (format: condition_id|slug|volume, one per line; header line skipped) ---
if [ -n "$LOAD_CSV" ]; then
  [ ! -f "$LOAD_CSV" ] && { echo "File not found: $LOAD_CSV"; exit 1; }
  export PGHOST="${DB_HOST:?Set DB_HOST}"
  export PGPASSWORD="${DB_PASSWORD:?Set DB_PASSWORD}"
  echo "Loading from $LOAD_CSV and updating polygon_markets.volume..."
  n=0
  tail -n +2 "$LOAD_CSV" | while IFS='|' read -r condition_id slug volume; do
    [ -z "$condition_id" ] && continue
    [ -z "$volume" ] || [ "$volume" = "null" ] && continue
    cid_escaped=$(echo "$condition_id" | sed "s/'/''/g")
    psql -t -A -c "UPDATE polygon_markets SET volume = $volume WHERE condition_id = '$cid_escaped';" 2>/dev/null > /dev/null
    n=$((n + 1))
    [ $((n % 500)) -eq 0 ] && echo "Updated $n rows..."
  done
  echo "Done. Updated DB from $LOAD_CSV"
  exit 0
fi

# --- Phase 1: Fetch from Gamma and write CSV ---
export PGHOST="${DB_HOST:?Set DB_HOST}"
export PGPASSWORD="${DB_PASSWORD:?Set DB_PASSWORD}"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "Exporting market slugs (volume IS NULL, OFFSET=$OFFSET${LIMIT:+ LIMIT $LIMIT})..."
if [ -n "$LIMIT" ] && [ "$LIMIT" -gt 0 ] 2>/dev/null; then
  if [ -n "$OFFSET" ] && [ "$OFFSET" -ge 0 ] 2>/dev/null; then
    psql -t -A -F'|' -c "SELECT condition_id, slug FROM polygon_markets WHERE slug IS NOT NULL AND slug != '' AND volume IS NULL ORDER BY condition_id OFFSET $OFFSET LIMIT $LIMIT;" > "$tmp/slugs.txt"
  else
    psql -t -A -F'|' -c "SELECT condition_id, slug FROM polygon_markets WHERE slug IS NOT NULL AND slug != '' AND volume IS NULL ORDER BY condition_id LIMIT $LIMIT;" > "$tmp/slugs.txt"
  fi
else
  if [ -n "$OFFSET" ] && [ "$OFFSET" -ge 0 ] 2>/dev/null; then
    psql -t -A -F'|' -c "SELECT condition_id, slug FROM polygon_markets WHERE slug IS NOT NULL AND slug != '' AND volume IS NULL ORDER BY condition_id OFFSET $OFFSET;" > "$tmp/slugs.txt"
  else
    psql -t -A -F'|' -c "SELECT condition_id, slug FROM polygon_markets WHERE slug IS NOT NULL AND slug != '' AND volume IS NULL ORDER BY condition_id;" > "$tmp/slugs.txt"
  fi
fi

total=$(wc -l < "$tmp/slugs.txt")
echo "Found $total markets. Fetching from Gamma and writing to $OUT_CSV ($WORKERS workers)."

[ "$total" -eq 0 ] && { echo "Nothing to do."; exit 0; }

# CSV header (pipe-delimited so slugs with commas are safe)
echo "condition_id|slug|volume" > "$OUT_CSV"

# Split into one file per worker (round-robin)
for ((w=0; w<WORKERS; w++)); do : > "$tmp/q$w"; done
n=0
while IFS= read -r line; do
  [ -z "$line" ] && continue
  echo "$line" >> "$tmp/q$((n % WORKERS))"
  n=$((n + 1))
done < "$tmp/slugs.txt"

progress_file="$tmp/progress"
: > "$progress_file"

process_chunk() {
  local qfile="$1"
  local outfile="$2"
  local condition_id slug slug_enc vol
  while IFS='|' read -r condition_id slug; do
    [ -z "$condition_id" ] && continue
    slug_enc=$(echo -n "$slug" | jq -sRr @uri)
    vol=$(curl -sS --max-time 15 "${GAMMA_URL}/markets/slug/${slug_enc}" 2>/dev/null | jq -r '.volumeNum // .volume // empty' 2>/dev/null)
    if [ -n "$vol" ] && [ "$vol" != "null" ]; then
      echo "${condition_id}|${slug}|${vol}" >> "$outfile"
    fi
    echo 1 >> "$progress_file"
  done < "$qfile"
}

# Start workers (each writes to its own file to avoid concurrent append)
for ((w=0; w<WORKERS; w++)); do
  [ -s "$tmp/q$w" ] && process_chunk "$tmp/q$w" "$tmp/out_$w.csv" &
done

# Progress loop
while true; do
  done_count=$(wc -l < "$progress_file" 2>/dev/null || echo 0)
  printf "\rProgress: %d / %d (%.1f%%)" "$done_count" "$total" "$(awk "BEGIN {printf \"%.1f\", ($done_count/$total)*100}")"
  [ "$done_count" -ge "$total" ] && break
  sleep 2
done

wait

# Merge worker CSVs into final CSV (after header already in OUT_CSV)
for ((w=0; w<WORKERS; w++)); do
  [ -f "$tmp/out_$w.csv" ] && cat "$tmp/out_$w.csv" >> "$OUT_CSV"
done

echo ""
echo "Fetch done. Wrote $(tail -n +2 "$OUT_CSV" | wc -l) rows to $OUT_CSV"

if [ -z "$FETCH_ONLY" ]; then
  echo "Updating DB from $OUT_CSV..."
  LOAD_CSV="$OUT_CSV" DB_HOST="$PGHOST" DB_PASSWORD="$PGPASSWORD" "$0"
else
  echo "Skipping DB update (FETCH_ONLY=1). To load later: LOAD_CSV=$OUT_CSV DB_HOST=... PGPASSWORD=... $0"
fi
