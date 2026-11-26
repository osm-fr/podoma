#!/bin/bash
set -e
set -x
cd $(dirname "$0")/..
mkdir -p logs/update_daily

LOG_FILE="logs/update_daily/$(date -Is).log"
nohup docker compose run --rm pdm update_daily $1 &> "$LOG_FILE" && echo "SUCCESS" || echo "FAILURE" &
tail -f "$LOG_FILE"
