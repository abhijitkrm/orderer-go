#!/usr/bin/env bash
# test.sh — the full suite, as the spec repo's verify.sh runs it.
set -euo pipefail
cd "$(dirname "$0")/.."
go vet ./...
go test ./...
go test -race -count=1 ./...
go run ./examples/quickstart > /dev/null
CHECKED=1 scripts/build-harness.sh
spec/conformance.sh harness/bin vectors
