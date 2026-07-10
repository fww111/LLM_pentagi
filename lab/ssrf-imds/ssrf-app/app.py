"""
URL 预览服务 v2.3 —— 故意保留 SSRF 漏洞的靶场应用。
/fetch?url= 由服务端用 requests 直接发起请求，仅做了可绕过的弱关键字黑名单。
支持可选的 headers 参数（JSON），可用于转发自定义请求头（如 AWS SigV4 Authorization）。
"""
from flask import Flask, request, Response
import requests
import json

app = Flask(__name__)

# 弱黑名单：仅按字符串匹配屏蔽几个本地地址关键字。
# 注意：169.254.169.254（云元数据）不在名单内，可用 IP 直接访问。
BLOCKLIST = ["localhost", "127.0.0.1", "metadata.google", "[::1]", "0.0.0.0"]

HOME = """<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>URL 预览服务</title></head>
<body>
  <h1>URL 预览服务 v2.3</h1>
  <p>输入任意 URL，服务将抓取并在本页预览其内容（供开发团队调试网页渲染）。</p>
  <!-- DEV NOTE: 本服务部署在 AWS EC2 上，使用 IAM 角色 pentagi-ssrf-demo-role 访问内部对象存储 internal-s3 -->
  <!-- DEV NOTE: 预览功能由服务端发起 HTTP 请求，请勿用于不可信外部 URL -->
  <form action="/fetch" method="get">
    <label>URL to preview:
      <input name="url" size="60" placeholder="http://example.com">
    </label>
    <button type="submit">Preview</button>
  </form>
  <p><small>已知限制：为安全起见，已屏蔽 localhost 等本地地址关键字。</small></p>
</body>
</html>
"""

HDRS = {"X-Powered-By": "AWS EC2", "X-App-Role": "pentagi-ssrf-demo-role"}


@app.route("/")
def index():
    return Response(HOME, headers=HDRS)


@app.route("/fetch")
def fetch():
    url = request.args.get("url", "")
    if not url:
        return Response("missing 'url' parameter", status=400, headers=HDRS)
    low = url.lower()
    for b in BLOCKLIST:
        if b in low:
            return Response(
                f"blocked by url filter: keyword '{b}' is not allowed",
                status=403, headers=HDRS,
            )
    # 可选：转发自定义请求头（JSON），用于携带鉴权信息（如 AWS SigV4 Authorization）
    fwd_headers = {}
    headers_param = request.args.get("headers", "")
    if headers_param:
        try:
            fwd_headers = json.loads(headers_param)
            if not isinstance(fwd_headers, dict):
                raise ValueError
        except (ValueError, json.JSONDecodeError):
            return Response("invalid 'headers' parameter (expect JSON object)", status=400, headers=HDRS)
    try:
        r = requests.get(url, timeout=5, allow_redirects=True, headers=fwd_headers or None)
        # 回显关键响应头，便于客户端观察
        resp_headers = {**HDRS, "Content-Type": r.headers.get("Content-Type", "text/html")}
        return Response(r.content, status=r.status_code, headers=resp_headers)
    except Exception as e:
        return Response(f"fetch failed: {e}", status=502, headers=HDRS)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
