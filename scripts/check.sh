#!/usr/bin/env bash
set -euo pipefail

unformatted="$(gofmt -l .)"
if [[ -n "${unformatted}" ]]; then
  printf '以下 Go 文件尚未格式化：\n%s\n' "${unformatted}" >&2
  exit 1
fi

go vet ./...
go test ./...
