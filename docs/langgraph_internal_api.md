# Go 与 LangGraph 内部接口协议

本文档描述阶段 0 中 Go 平台层与 Python LangGraph 编排层之间的最小接口边界。前端只访问 Go；Python 只通过内部网络调用 Go 暴露的内部 API。

## 服务边界

- Go 保留鉴权、数据库、Docker 工具执行、Workspace、Artifact、Provider 配置、事件落库和前端推送。
- Python LangGraph 负责状态图、节点路由、计划分解、interrupt 恢复、checkpoint 和最终汇总。
- Go 调 Python 使用 `LANGGRAPH_URL`。
- Python 调 Go 使用 `GO_INTERNAL_URL`，推荐容器内地址为 `http://pentagi:8081`。

## 认证

所有 `/internal/*` 写入或敏感接口都必须携带：

```http
X-Internal-Key: ${INTERNAL_API_KEY}
```

约束：

- `INTERNAL_API_KEY` 不写入 `runtime_events`、`msglogs`、普通应用日志或报告。
- `resolved_auth` 仅允许 Python 进程内存态使用，禁止落库和打印。
- 生产模式下必须配置 32 位以上的 `INTERNAL_API_KEY`。

## Go -> Python

### POST `/runs/start`

启动一个 LangGraph 运行。

```json
{
  "task_id": 1,
  "context_id": "context-uuid",
  "content": "user task content",
  "data": {
    "flow_id": 1
  },
  "provider": {
    "provider_type": "openai",
    "model": "gpt-4.1",
    "supports_tools": true
  },
  "workspace": {
    "flow_id": 1,
    "task_id": 1,
    "context_id": "context-uuid",
    "host_path": "/data/workspaces/context-uuid",
    "container_path": "/workspace"
  }
}
```

### POST `/runs/resume`

恢复 `INPUT_REQUIRED` 或 `AUTH_REQUIRED` 中断后的运行。

```json
{
  "context_id": "context-uuid",
  "resume_payload": {}
}
```

### POST `/runs/cancel`

取消运行。

```json
{
  "context_id": "context-uuid",
  "reason": "user canceled"
}
```

### GET `/state/{context_id}`

读取 LangGraph 当前状态快照。

## Python -> Go

### POST `/internal/tools/execute`

通过 Go 复用现有工具执行链。

```json
{
  "tool_name": "terminal",
  "params": {},
  "task_id": 1,
  "context_id": "context-uuid"
}
```

响应：

```json
{
  "output": "...",
  "error": null
}
```

### POST `/internal/runtime-events`

写入统一运行时事件，供 Go 落库并推送到前端。

```json
{
  "flow_id": 1,
  "task_id": 1,
  "context_id": "context-uuid",
  "seq": 1,
  "event_type": "WORKING",
  "node_name": "pentester",
  "is_final": false,
  "payload": {},
  "error_code": null,
  "retryable": null,
  "timestamp": "2026-05-14T00:00:00Z"
}
```

### POST `/internal/provider-config/resolve`

由 Go 解析当前任务可用 Provider 配置。该接口可能返回内存态认证信息，只能供 Python 调用模型时临时使用。

```json
{
  "flow_id": 1,
  "task_id": 1,
  "context_id": "context-uuid"
}
```

响应：

```json
{
  "provider_name": "openai",
  "provider_type": "openai",
  "model": "gpt-4.1",
  "server_url": "https://api.openai.com/v1",
  "resolved_auth": {
    "api_key": "***"
  },
  "configured": true
}
```

### POST `/internal/workspace/write`

通过 Go 写入 Workspace 文件。Go 必须校验 `relative_path`，禁止路径穿越。

```json
{
  "task_id": 1,
  "context_id": "context-uuid",
  "relative_path": "artifacts/result.md",
  "content": "..."
}
```

### POST `/internal/artifacts/register`

注册报告、截图、脚本、验证结果等产物。

```json
{
  "task_id": 1,
  "context_id": "context-uuid",
  "name": "final_report.md",
  "artifact_type": "text",
  "content": "..."
}
```

## 阶段 0 状态

阶段 0 只建立安全边界、接口形状和最小可编译实现。工具执行、Workspace 写入、Artifact 注册等接口可以先返回 `501 Not Implemented`，但路由、鉴权、请求体限制、Provider 配置解析和 LangGraph Client 类型必须先稳定下来。
