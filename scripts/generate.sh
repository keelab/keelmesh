#!/usr/bin/env bash
set -euo pipefail

for command in buf; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    printf '缺少生成工具：%s\n' "${command}" >&2
    exit 1
  fi
done

buf dep update
buf lint
buf generate
cp gen/*/v1/*.openapi.json api/openapi/
