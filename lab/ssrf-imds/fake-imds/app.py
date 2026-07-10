"""
模拟 AWS EC2 Instance Metadata Service (IMDSv1)。
绑定在 169.254.169.254，返回 IAM 临时凭证（与 internal-s3 校验值一致）。
IMDSv1 无需额外请求头即可 GET，是 SSRF 横向的高价值目标。
"""
from flask import Flask, Response
import json

app = Flask(__name__)

CREDENTIALS = {
    "Code": "Success",
    "LastUpdated": "2026-07-10T00:00:00Z",
    "Type": "AWS-HMAC",
    "AccessKeyId": "AKIA4SSRFDEMO2026XY",
    "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYSSRFDEMOKEY",
    "Token": "IQOJEXAMPLETOKENSSRFDEMO2026LATERALMOVEMENT",
    "Expiration": "2026-12-31T23:59:59Z",
}

# IMDS 目录树（纯文本，换行分隔子项 —— 与真实 AWS 行为一致）
TREE = {
    "/latest": "meta-data/\nuser-data\n",
    "/latest/meta-data/": "ami-id\nami-launch-index\nami-manifest-path\nhostname\ninstance-id\ninstance-type\nlocal-ipv4\niam/\nplacement/\n",
    "/latest/meta-data/iam/": "info/\nsecurity-credentials/\n",
    "/latest/meta-data/iam/info/": json.dumps({
        "Code": "Success",
        "InstanceProfileArn": "arn:aws:iam::123456789012:instance-profile/pentagi-ssrf-demo-role",
        "LastUpdated": "2026-07-10T00:00:00Z",
    }),
    "/latest/meta-data/iam/security-credentials/": "pentagi-ssrf-demo-role\n",
    "/latest/meta-data/iam/security-credentials/pentagi-ssrf-demo-role": json.dumps(CREDENTIALS, indent=2),
    "/latest/meta-data/instance-id": "i-ssrfdemo2026abcd0123",
    "/latest/meta-data/instance-type": "t3.medium",
    "/latest/meta-data/local-ipv4": "172.20.0.10",
    "/latest/meta-data/hostname": "ip-172-20-0-10.ec2.internal",
    "/latest/meta-data/placement/": "availability-zone\n",
    "/latest/meta-data/placement/availability-zone": "us-east-1a",
    "/latest/user-data": "#!/bin/bash\necho bootstrap-complete",
}


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def imds(path):
    full = "/" + path
    body = TREE.get(full)
    if body is None:
        return Response("404 Not Found", status=404, mimetype="text/plain")
    return Response(body, mimetype="text/plain")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)
