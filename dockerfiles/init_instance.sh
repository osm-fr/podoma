#!/bin/bash
set -e
set -x
cd $(dirname "$0")/..
mkdir -p logs/init


docker compose up -d pgsqldb
sleep 15
docker compose build
docker compose run --rm pdm install

LOG_FILE="logs/init/$(date -Is).log"
nohup docker compose run --rm pdm init_instance &> "$LOG_FILE" && echo "SUCCESS" || echo "FAILURE" &
tail -f "$LOG_FILE"
