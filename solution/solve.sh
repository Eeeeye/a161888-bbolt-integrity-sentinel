#!/bin/bash
set -euo pipefail

cd /app
git apply --check /solution/activity161888-repair.patch
git apply /solution/activity161888-repair.patch
gofmt -w bucket.go tx.go tx_check.go internal/surgeon/xray.go
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go build . ./internal/surgeon
