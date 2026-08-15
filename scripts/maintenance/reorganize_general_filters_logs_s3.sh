#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Reorganize TG Alerts general-filter logs in Amazon S3
#
# Old layout:
#   general-filters-logs/run_date=YYYY-MM-DD/run_time=HHMM/
#     run_summary.csv
#     symbol_processing_log.csv
#
# New layout:
#   general-filters-logs/run_summary/run_date=YYYY-MM-DD/run_time=HHMM/
#     run_summary.csv
#
#   general-filters-logs/symbol_processing_log/run_date=YYYY-MM-DD/run_time=HHMM/
#     symbol_processing_log.csv
#
# Safety:
#   - Files are copied first.
#   - New locations are listed for verification.
#   - Old-file deletion runs as a dry run by default.
#   - Change EXECUTE_DELETE=false to true only after verification.
# ============================================================

BUCKET="bin-tickers-yev"
ROOT="s3://${BUCKET}/general-filters-logs"
EXECUTE_DELETE=false

echo
echo "[1/5] Copying run_summary.csv files..."
aws s3 cp "${ROOT}/" "${ROOT}/run_summary/" \
  --recursive \
  --exclude "*" \
  --include "run_date=*/run_time=*/run_summary.csv"

echo
echo "[2/5] Copying symbol_processing_log.csv files..."
aws s3 cp "${ROOT}/" "${ROOT}/symbol_processing_log/" \
  --recursive \
  --exclude "*" \
  --include "run_date=*/run_time=*/symbol_processing_log.csv"

echo
echo "[3/5] Verifying new run_summary location..."
aws s3 ls "${ROOT}/run_summary/" --recursive

echo
echo "[4/5] Verifying new symbol_processing_log location..."
aws s3 ls "${ROOT}/symbol_processing_log/" --recursive

echo
echo "[5/5] Checking old files that would be deleted..."
aws s3 rm "${ROOT}/" \
  --recursive \
  --exclude "*" \
  --include "run_date=*/run_time=*/run_summary.csv" \
  --include "run_date=*/run_time=*/symbol_processing_log.csv" \
  --dryrun

if [[ "${EXECUTE_DELETE}" == "true" ]]; then
  echo
  echo "WARNING: EXECUTE_DELETE=true. Deleting old copies now..."
  aws s3 rm "${ROOT}/" \
    --recursive \
    --exclude "*" \
    --include "run_date=*/run_time=*/run_summary.csv" \
    --include "run_date=*/run_time=*/symbol_processing_log.csv"

  echo
  echo "Final S3 layout:"
  aws s3 ls "${ROOT}/" --recursive
else
  echo
  echo "No files were deleted."
  echo "Review the dry-run output above."
  echo "To remove the old copies, edit this file and set:"
  echo "    EXECUTE_DELETE=true"
fi

echo
echo "Migration script completed."
