#!/usr/bin/env bash
# 对本地 PostgreSQL 16 实例跑全部端到端自测：
#   selftest.py           诊断 / 终止会话 / VACUUM 作业 / 重启核实
#   logstream_selftest.py 日志边采集边保存 / 游标续读 / 中断续航
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOCKET="${PG_SOCKET:-/workspace/.tools/run}"
PORT="${PG_PORT:-55432}"

export DATABASE_URL="postgresql://postgres@/devicedb?host=${SOCKET}&port=${PORT}"
export PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

# 注意: 这里运行的是仓库内自有代码，不读取任何下载归档，
# 因此不使用 -I（-I 会同时忽略 user site 中的 psycopg/fastapi）。
python3 "${ROOT}/tests/selftest.py" "$@"
python3 "${ROOT}/tests/logstream_selftest.py" "$@"
