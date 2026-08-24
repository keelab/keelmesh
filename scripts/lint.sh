#!/usr/bin/env bash
set -euo pipefail

if ! command -v golangci-lint >/dev/null 2>&1; then
  printf '缺少 golangci-lint 命令。\n' >&2
  exit 1
fi

golangci-lint run
