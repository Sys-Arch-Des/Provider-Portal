#!/usr/bin/env bash
# Adds an issue or PR to the project board (if missing) and sets its Status.
# Usage: set-status.sh <issue-or-pr-url> <status>
# Env: GH_TOKEN, PROJECT_OWNER, PROJECT_NUMBER
set -euo pipefail

url="$1"
status="$2"

project_id=$(gh project view "$PROJECT_NUMBER" --owner "$PROJECT_OWNER" --format json --jq .id)
item_id=$(gh project item-add "$PROJECT_NUMBER" --owner "$PROJECT_OWNER" --url "$url" --format json --jq .id)

field=$(gh project field-list "$PROJECT_NUMBER" --owner "$PROJECT_OWNER" --format json \
  --jq '.fields[] | select(.name == "Status")')
field_id=$(jq -r .id <<<"$field")
option_id=$(jq -r --arg s "$status" '.options[] | select(.name == $s) | .id' <<<"$field")

if [ -z "$option_id" ]; then
  echo "Status option '$status' not found on the board" >&2
  exit 1
fi

gh project item-edit --project-id "$project_id" --id "$item_id" \
  --field-id "$field_id" --single-select-option-id "$option_id"
echo "$url -> $status"
