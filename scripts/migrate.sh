#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${DATABASE_URL:-}" ]]; then
  printf '必须通过环境变量 DATABASE_URL 提供数据库连接。\n' >&2
  exit 1
fi
if ! command -v migrate >/dev/null 2>&1; then
  printf '缺少 migrate 命令，请安装 golang-migrate。\n' >&2
  exit 1
fi

direction="${1:-}"
case "${direction}" in
  up)
    migrate -path migrations -database "${DATABASE_URL}" up
    ;;
  down)
    steps="${2:-1}"
    migrate -path migrations -database "${DATABASE_URL}" down "${steps}"
    ;;
  *)
    printf '用法：%s <up|down> [steps]\n' "$0" >&2
    exit 2
    ;;
esac
