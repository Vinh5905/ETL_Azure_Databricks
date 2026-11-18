#!/usr/bin/env bash
set -euo pipefail
: "${LAKE_ACCOUNT:?Đặt LAKE_ACCOUNT theo output của stack 10-core}"
export AZCOPY_AUTO_LOGIN_TYPE=AZCLI
root="$(git rev-parse --show-toplevel)"
destination="$root/data/ds/v1/product"
mkdir -p "$destination"
for scale in 10 25 50 100; do
  mkdir -p "$destination/scale=$scale"
  azcopy copy "https://${LAKE_ACCOUNT}.blob.core.windows.net/ds-data/exports/v1/product/scale=${scale}/*.txt" \
    "$destination/scale=$scale/" --recursive=false
done
azcopy copy "https://${LAKE_ACCOUNT}.blob.core.windows.net/ds-data/exports/v1/_manifest.json" "$root/data/ds/v1/"
