#!/bin/sh
set -e
# 把 AWS EC2 link-local 元数据地址 169.254.169.254 绑定到本容器 eth0，
# 使同网段其他容器可通过 ARP 解析到本容器（模拟 IMDSv1）。
if ip addr add 169.254.169.254/32 dev eth0 2>/dev/null; then
    echo "[imds] bound 169.254.169.254/32 on eth0"
else
    echo "[imds] WARN: could not add 169.254.169.254/32 (already bound or lacking NET_ADMIN?)"
fi
ip -4 addr show eth0 | grep inet || true
echo "[imds] starting IMDSv1 emulator on :80"
exec python /app/app.py
