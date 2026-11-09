#!/usr/bin/env bash
#
# 压测 + pprof 采集 + 分析（一条命令完成）
# 用法: STRESS_PASS=123456 ./profil.sh [seconds] [concurrency] [limit]
#
# 流程:
#   ① 后台采集 CPU（覆盖压测窗口）
#   ② 前台压测（与采集同时进行）
#   ③ 等待 CPU 采集完成
#   ④ heap 快照（压测刚结束）
#   ⑤ 输出 CPU/Heap Top 20
set -euo pipefail

DURATION_SECS="${1:-30}"
CONCURRENCY="${2:-50}"
LIMIT="${3:-20}"

CPU_OUT="${CPU_OUT:-/tmp/cpu.pprof}"
HEAP_OUT="${HEAP_OUT:-/tmp/heap.pprof}"
PPROF_URL="${PPROF_URL:-http://127.0.0.1:6060}"

# 脚本所在目录（保证从任意目录运行都能找到 feed.sh）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------- 前置检查：pprof 是否可达 ----------
if ! curl -sf "$PPROF_URL/debug/pprof/" >/dev/null; then
  echo "错误：pprof 不可达（$PPROF_URL）"
  echo "请确认已启用 pprof 接入（cmd/main.go 中 /debug/pprof）"
  exit 1
fi

# ---------- ① 后台采集 CPU ----------
# & 表示后台启动，不阻塞；$! 是最后一个后台进程（& 启动）的 PID
echo "=== 开始 CPU 采集（${DURATION_SECS}s，后台） ==="
curl -s -o "$CPU_OUT" "$PPROF_URL/debug/pprof/profile?seconds=$DURATION_SECS" &
CPUPID=$!

# ---------- ② 前台压测（与采集同时） ----------
echo "=== 压测中（${DURATION_SECS}s × ${CONCURRENCY} 并发） ==="
"$SCRIPT_DIR/feed.sh" "$CONCURRENCY" "${DURATION_SECS}s" "$LIMIT"

# ---------- ③ 等 CPU 采集完成 ----------
wait "$CPUPID"

# ---------- ④ heap 快照 ----------
echo "=== 采集 Heap ==="
curl -s -o "$HEAP_OUT" "$PPROF_URL/debug/pprof/heap"

echo
echo "=== 采集完成 ==="
echo "  CPU : $CPU_OUT"
echo "  Heap: $HEAP_OUT"
echo
echo "=== CPU Top 20 ==="
go tool pprof -top -nodecount=20 "$CPU_OUT"
echo
echo "=== Heap Top 20（alloc_space，谁在制造垃圾） ==="
go tool pprof -top -nodecount=20 -sample_index=alloc_space "$HEAP_OUT"
