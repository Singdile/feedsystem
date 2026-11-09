#!/usr/bin/env bash
#
# Feed 接口压测脚本
# 用法: ./feed.sh [concurrency] [duration] [limit]
# 示例: STRESS_PASS=123456 ./feed.sh 50 30s 20
#
# set 选项:
#   -e          任一命令返回非 0 退出码（失败）时立即退出
#   -u          使用未定义变量时立即报错退出
#   -o pipefail 管道中任一命令失败，整条管道返回失败码
set -euo pipefail

# ---------- 依赖检查 ----------
for cmd in curl jq hey; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "缺少依赖: $cmd"; exit 1; }
done

# ---------- 变量定义 ----------
# ${VAR:-default} 未定义或为空 → 用默认值
# ${VAR:?msg}     未定义或为空 → 报错退出
BASE_URL="${BASE_URL:-http://localhost:8080}"
USERNAME="${STRESS_USER:-sdl}"
PASSWORD="${STRESS_PASS:-123456}"   # 本地临时固定；勿提交到公开仓库
CONCURRENCY="${1:-50}"             # 并发数
DURATION="${2:-30s}"               # 持续时间
LIMIT="${3:-20}"                   # 每页条数

# ---------- 登录拿 token ----------
# $(command): 执行命令并捕获 stdout
# jq -r    : 输出原始字符串（不加 -r 会带双引号，导致 Bearer "xxx" 认证失败）
# // empty : access_token 不存在时输出空串而非 null
TOKEN=$(curl -s -X POST "$BASE_URL/api/v1/auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"$USERNAME\",\"password\":\"$PASSWORD\"}" \
  | jq -r '.data[0].access_token // empty')

# [ ] 表示 test 命令， -z 判断长度是否为0
if [ -z "$TOKEN" ]; then
  echo "登录失败：请检查 STRESS_USER / STRESS_PASS"
  exit 1
fi

# ---------- 预热 ----------
# -s 静默；-o /dev/null 丢弃响应体（只为让缓存热起来）
echo "=== 预热 3 次 ==="
for _ in 1 2 3; do
  curl -s -o /dev/null "$BASE_URL/api/v1/feed?limit=$LIMIT" -H "Authorization: Bearer $TOKEN"
done

# ---------- 压测 ----------
# -z 持续时间；-c 并发数；-m 方法；-H 请求头
echo "=== 压测: ${DURATION} × ${CONCURRENCY} 并发（limit=${LIMIT}）==="
hey -z "$DURATION" -c "$CONCURRENCY" -m GET \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/api/v1/feed?limit=$LIMIT"
