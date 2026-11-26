#!/bin/bash
# Init projects added to an already initialized instance, then restart the server to load them.
# Usage: ./dockerfiles/init_new_project.sh [keep]
set -e
set -x
cd $(dirname "$0")/..
mkdir -p logs/init_new_project

LOG_FILE="logs/init_new_project/$(date -Is).log"
nohup bash -c "docker compose run --rm pdm init_new_projects $1 && docker compose restart pdm" &> "$LOG_FILE" && echo "SUCCESS" || echo "FAILURE" &
tail -f "$LOG_FILE"
