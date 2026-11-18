#!/usr/bin/env bash
set -euo pipefail
: "${LAKE_ACCOUNT:?Đặt LAKE_ACCOUNT theo output của stack 10-core}"
export AZCOPY_AUTO_LOGIN_TYPE=AZCLI
root="$(git rev-parse --show-toplevel)"
file="$root/local/hadoop/results/e6.jsonl"
test -s "$file"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
azcopy copy "$file" "https://${LAKE_ACCOUNT}.blob.core.windows.net/ds-data/inbox/local/e6_${stamp}.jsonl"
