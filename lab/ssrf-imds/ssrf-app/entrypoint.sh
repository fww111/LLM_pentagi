#!/bin/sh
set -e
# Docker Desktop 下 link-local 地址(169.254.169.254)的 ARP 不被同网段容器转发，
# 故用 DNAT 把"服务端出向到云元数据 169.254.169.254:80"的流量重定向到 fake-imds 容器，
# 使 SSRF 的服务端请求真正抵达我们的 IMDS 模拟器。
# 注意：iptables 不能可靠解析服务名，且容器启动早期 DNS 可能未就绪，所以先解析 IP（带重试）。
IMDS_IP=""
for i in 1 2 3 4 5 6 7 8 9 10; do
    IMDS_IP=$(python3 -c "import socket;print(socket.gethostbyname('fake-imds'))" 2>/dev/null || true)
    [ -n "$IMDS_IP" ] && break
    sleep 1
done
if [ -n "$IMDS_IP" ]; then
    if iptables -t nat -A OUTPUT -d 169.254.169.254/32 -p tcp --dport 80 -j DNAT --to-destination "$IMDS_IP:80" 2>/dev/null; then
        echo "[ssrf-app] DNAT installed: 169.254.169.254:80 -> $IMDS_IP:80 (fake-imds)"
    else
        echo "[ssrf-app] WARN: iptables DNAT failed (need NET_ADMIN?)"
    fi
else
    echo "[ssrf-app] WARN: could not resolve fake-imds after retries; no DNAT"
fi
echo "[ssrf-app] starting URL preview service on :8000"
exec python /app/app.py
