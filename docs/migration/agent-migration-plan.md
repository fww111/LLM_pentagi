# PentAGI Agent 改造方案 — 执行流程驱动

> 基于《多智能体协同与调度规范 v1.0》，按执行流程描述改造后的完整系统。
> 核心原则：直接替换原编排层，不保留旧链路，不建兼容映射。

---

## 目录

- [1. 新系统执行流程](#1-新系统执行流程)
- [2. LangGraph 图拓扑](#2-langgraph-图拓扑)
- [3. 各节点详细设计](#3-各节点详细设计)
  - [3.1 Designer — 需求澄清](#31-designer--需求澄清)
  - [3.2 Planner — 计划生成与调整](#32-planner--计划生成与调整)
  - [3.3 Supervisor — LLM 智能调度](#33-supervisor--llm-智能调度)
  - [3.4 Builder — 环境准备](#34-builder--环境准备)
  - [3.5 Generator — 代码生成](#35-generator--代码生成)
  - [3.6 Integrator — 代码集成](#36-integrator--代码集成)
  - [3.7 Tester — 代码验证](#37-tester--代码验证)
  - [3.8 Pentester — 攻击执行](#38-pentester--攻击执行)
  - [3.9 Reviewer — 质量审查](#39-reviewer--质量审查)
  - [3.10 Reporter — 报告输出](#310-reporter--报告输出)
  - [3.11 辅助节点 — input_required / auth_required / rejected / failed](#311-辅助节点--input_required--auth_required--rejected--failed)
- [4. 共享服务层（不改动）](#4-共享服务层不改动)
- [5. 代码改造清单](#5-代码改造清单)
- [6. TaskState 定义](#6-taskstate-定义)
- [7. 规范状态模型](#7-规范状态模型)
- [8. Workspace 规范](#8-workspace-规范)
- [9. 消息、产物与流式协议](#9-消息产物与流式协议)
- [10. 结构化错误](#10-结构化错误)
- [11. CodeState 生命周期分析](#11-codestate-生命周期分析)
- [12. 工具权限矩阵](#12-工具权限矩阵)
- [13. 实施顺序](#13-实施顺序)

---

## 1. 新系统执行流程

改造后的完整执行流程与原项目结构一致，共五步：

```
┌──────────────────────────────────────────────────────────────────────┐
│  第一步：初始化（不变）                                               │
├──────────────────────────────────────────────────────────────────────┤
│  1. 用户创建 Flow（渗透测试流程）                                     │
│  2. FlowWorker 创建 Docker 沙箱容器                                   │
│  3. FlowWorker → 创建 Task，TaskWorker 启动                           │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│  第二步：需求澄清                                                    │
├──────────────────────────────────────────────────────────────────────┤
│  4. Designer Agent 把用户输入规范化为 scope_contract                  │
│     • 如果信息不完整 → ask_user → 等待用户补充                        │
│     • 完整后 → 输出结构化的 scope_contract                            │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│  第三步：计划生成                                                    │
├──────────────────────────────────────────────────────────────────────┤
│  5. Planner Agent 基于 scope_contract 生成 Todo 列表                  │
│     • 每个 Todo 含 owner_agent、依赖、风险等级、成功标准               │
│     • 最多生成 15 个 Todo                                             │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│  第四步：循环执行每个 Todo                                            │
├──────────────────────────────────────────────────────────────────────┤
│  6. Supervisor 读取当前状态 → 决定下一步执行哪个 Agent                 │
│     ┌── Builder：环境准备（if need_env）                              │
│     ├── Generator：PoC/脚本生成（if need_code）                       │
│     ├── Integrator：代码落盘+元数据（代码已生成后）                    │
│     ├── Tester：代码验证（代码已集成后）                               │
│     │   └─ 失败 → Supervisor 路由回对应 Agent 修复                    │
│     ├── Pentester：攻击执行（核心）                                   │
│     │   └─ 多轮执行，每轮回 Supervisor 判断是否继续                     │
│     │   └─ 高风险操作 → auth_required → 用户授权/拒绝                  │
│     ├── 策略拒绝 → rejected → END                                     │
│     ├── 不可恢复错误 → failed → END                                   │
│     └── Reviewer 退回 → Supervisor 路由到指定 Agent 修复              │
│                                                                      │
│  7. 每个 Agent 完成后 → 回到 Supervisor → 判断下一步                  │
│  8. 一批 Todo 完成后 → Planner 可调整剩余计划（增/删/改 Todo）          │
│  9. 回到循环顶部，处理下一个 Todo                                     │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│  第五步：收尾                                                        │
├──────────────────────────────────────────────────────────────────────┤
│  10. 所有 Todo 完成 → Reviewer 审查质量和合规                         │
│  11. Reviewer 通过 → Reporter 生成最终报告                            │
│  12. TaskWorker 标记任务完成                                          │
└──────────────────────────────────────────────────────────────────────┘
```

### 与原项目对比

| 阶段 | 原项目 | 新系统 |
|------|--------|--------|
| **初始化** | Flow → Docker → Task → TaskWorker | 不变 |
| **第一步** | Generator 拆解为 Subtask | Designer 澄清需求 → Planner 生成 Todo |
| **循环** | Primary Agent 编排 → 委派 Agent | Supervisor 编排 → 委派 Agent |
| **调整** | Refiner 调整剩余 Subtask | Planner 调整剩余 Todo |
| **收尾** | Reporter 出报告 | Reviewer 审查 → Reporter 出报告 |

---

## 2. LangGraph 图拓扑

### 新图（直接替换旧图）

> 图状态使用 Section 7.4 的 `SharedAgentState`（Pydantic BaseModel）。以下代码中 `TaskState` 即 `SharedAgentState` 的别名。

```python
# 图状态 = 规范 SharedAgentState
builder = StateGraph(SharedAgentState)

# === 节点注册 ===
builder.add_node("designer", designer)
builder.add_node("planner", planner)
builder.add_node("supervisor", supervisor)
builder.add_node("builder", builder_node)
builder.add_node("generator", generator_node)
builder.add_node("integrator", integrator_node)
builder.add_node("tester", tester_node)
builder.add_node("pentester", pentester_node)
builder.add_node("reviewer", reviewer_node)
builder.add_node("reporter", reporter_node)
builder.add_node("input_required", input_required)
builder.add_node("auth_required", auth_required)
builder.add_node("rejected", rejected)
builder.add_node("failed", failed)

# === 边定义 ===

# 入口
builder.add_edge(START, "designer")

# Designer → 路由
builder.add_conditional_edges("designer", route_after_designer, {
    "planner": "planner",                        # scope_contract 完成
    "input_required": "input_required",          # 信息不完整，等用户
})
builder.add_edge("input_required", "designer")   # 用户回复后回到 designer

# Planner → Supervisor
builder.add_edge("planner", "supervisor")

# Supervisor → 路由到执行节点
builder.add_conditional_edges("supervisor", route_supervisor, {
    "builder": "builder",
    "generator": "generator",
    "integrator": "integrator",
    "tester": "tester",
    "pentester": "pentester",
    "planner": "planner",          # 需要调整计划
    "reviewer": "reviewer",        # 所有 Todo 完成
    "reporter": "reporter",        # Reviewer 已通过
    "input_required": "input_required",
    "auth_required": "auth_required",
    "rejected": "rejected",
    "failed": "failed",
    "end": END,
})

# 所有执行节点完成后回到 Supervisor
builder.add_edge("builder", "supervisor")
builder.add_edge("generator", "supervisor")
builder.add_edge("integrator", "supervisor")
builder.add_edge("tester", "supervisor")
builder.add_edge("pentester", "supervisor")
builder.add_edge("input_required", "supervisor")
builder.add_edge("auth_required", "supervisor")
builder.add_edge("rejected", END)
builder.add_edge("failed", END)

# Reviewer → 路由
builder.add_conditional_edges("reviewer", route_after_reviewer, {
    "reporter": "reporter",        # PASS
    "supervisor": "supervisor",    # FAIL → 退回执行
})

# Reporter → END
builder.add_edge("reporter", END)
```

### 图拓扑可视化

```
START
  └──► designer
         ├─ [信息不完整] ──► input_required ──► designer
         └─ [scope_contract 完成] ──► planner
                                       │
                                       ▼
                    supervisor ◄──────────────────────────────┐
                     │    │                                    │
                     │  ┌─┴─【快速路径：不调 LLM】              │
                     │  ├── scope_contract 缺失 → designer    │
                     │  ├── plan 缺失 → planner               │
                     │  └── reporter 完成 → END               │
                     │                                        │
                     │  ┌─┴─【慢路径：调 LLM 判断】            │
                     │  ├── need_env? ──────► builder ───────►│
                     │  ├── need_code? ─────► generator ─────►│
                     │  ├── 代码未集成? ────► integrator ────►│
                     │  ├── 代码未验证? ────► tester ─────────►│
                     │  │   └─ issue=code → generator ───────►│
                     │  │   └─ issue=env ──→ builder ─────────►│
                     │  ├── Todo 可执行? ──► pentester ──────►│
                     │  │   └─ AUTH_REQUIRED → auth_required ►│
                     │  │                        └──► supervisor
                     │  ├── 需调整计划? ───► planner ─────────►│
                     │  ├── 所有 Todo 完成? ► reviewer        │
                     │  │   └─ FAIL → supervisor（退回修复）──►│
                     │  ├── 策略拒绝? ────► rejected ──► END  │
                     │  ├── 不可恢复错误? ► failed ───► END   │
                     │  └── Review PASS ──► reporter ──► END  │
                     │                                        │
                     └── input_required ──► designer ─────────►│
```

---

## 3. 各节点详细设计

> 每个节点都包含「代码复用来源」一节，精确说明：
> - **Go 端复用**：哪个文件、哪个函数、是完全复用还是改造
> - **Prompt 复用**：哪个模板、保留什么、改什么
> - **新增代码量**：预估行数

---

### 3.1 Designer — 需求澄清

**执行时机**：Task 启动后第一个节点。

**职责**：把用户的自然语言输入规范化为结构化的 `scope_contract`。

**调用 Go 端**：
- Python 调 `_go_post("tasks/{id}/designer-step")`
- Go 端组装上下文，调 LLM，LLM 通过 `scope_contract` 工具输出结果

**输出的 scope_contract 结构**：
```json
{
  "test_objective": "对目标系统进行渗透测试",
  "authorized_scope": ["192.168.1.0/24", "web.example.com"],
  "forbidden_actions": ["不进行 DoS", "不测试生产环境"],
  "success_criteria": ["发现至少一个高危漏洞", "获取系统最高权限"],
  "deliverables": ["渗透测试报告", "漏洞清单", "PoC 脚本"],
  "missing_info": [],
  "environment_prerequisites": ["VPN 接入凭证"]
}
```

**信息不完整时**：触发 `ask_user` → 进入 `input_required` 节点 → 用户回复后回到 designer。

**状态变更输出（SharedAgentStatePatch）**：
- `decision`: 写入 scope_contract 的完整/不完整状态
- `message`: `role="Designer"`, `agent_status=COMPLETED/WORKING`
- `agent_status.designer`: `COMPLETED` 或 `WORKING`（信息不完整时）
- 如信息不完整：task status 设为 `INPUT_REQUIRED`

#### 代码复用来源

**Go 端 — 新增 `providers/designer.go`（约 120 行）**：

| 复用来源 | 文件 | 函数 | 复用方式 |
|---------|------|------|---------|
| Handler 模式 | `handlers.go:674` `searcherHandler()` | 两步走（question + execute） | **复制模式**——改 prompt 类型为 `PromptTypeDesigner` |
| Performer 模式 | `performers.go` `performSearcher()` | restoreChain + performAgentChain | **复制模式**——改 MsgChainType 为 `MsgchainTypeDesigner` |
| 搜索能力 | `handlers.go` `GetSubtaskSearcherHandler()` | 搜索子能力调用 | **直接调用**——DesignerExecutorConfig 包含 Searcher handler |
| 记忆能力 | `handlers.go` `GetMemoristHandler()` | 记忆子能力调用 | **直接调用**——DesignerExecutorConfig 包含 Memorist handler |
| Executor 构建 | `executor.go` `GetSearcherExecutor()` | 工具集配置 | **复制模式**——新增 `GetDesignerExecutor()` |
| 消息链管理 | `helpers.go` `restoreChain()` | chain 恢复/创建 | **直接复用**——不改 |
| LLM 调用链 | `performer.go` `callWithRetries()` | 重试机制 | **直接复用**——不改 |

**Executor 工具集**：
```go
type DesignerExecutorConfig struct {
    TaskID          *int64
    ScopeContract   tools.ExecutorHandler  // Barrier
    Searcher        tools.ExecutorHandler  // 委托 searcher
    Memorist        tools.ExecutorHandler  // 委托 memorist
    Ask             tools.ExecutorHandler  // Barrier
    Summarizer      tools.ExecutorHandler  // 内部
}
```

**Prompt — 新写 `prompts/designer.tmpl` + `prompts/question_designer.tmpl`（约 200 行）**：

| 参考来源 | 文件 | 参考内容 |
|---------|------|---------|
| 上下文装配 | `primary_agent.tmpl` | `<execution_context>` 解读方式——Designer 需要读取 Flow/Task 信息 |
| 信息检索原则 | `enricher.tmpl` | "只补事实不补意见"——Designer 用搜索工具补全目标信息时遵循此原则 |
| Question 模式 | `question_searcher.tmpl` | 简短问答模板——Designer 先判断需要什么信息再执行 |

**新增代码量**：
- Go 端：`providers/designer.go` ~120 行 + `executor.go` 新增 config ~40 行 + `registry.go` 新增工具 ~30 行
- Prompt：`designer.tmpl` ~150 行 + `question_designer.tmpl` ~50 行

---

### 3.2 Planner — 计划生成与调整

**执行时机**：
- 首次：Designer 输出 scope_contract 后
- 后续：Supervisor 判断需要调整计划时

**职责**：
- 首次调用：基于 scope_contract 生成 Todo 列表
- 后续调用：根据已完成的 Todo 结果，增/删/改剩余 Todo（类似原 Refiner）

**调用 Go 端**：
- 生成计划：`_go_post("tasks/{id}/generate-todo-plan")`
- 调整计划：`_go_post("tasks/{id}/refine-todo-plan")`

**状态变更输出（SharedAgentStatePatch）**：
- `todos`: 生成或调整后的完整 Todo 列表
- `message`: `role="Planner"`, `agent_status=COMPLETED`
- `agent_status.planner`: `COMPLETED`

**输出的 Todo 结构**：
```json
{
  "todo_id": "todo_001",
  "title": "端口扫描和服务识别",
  "owner_agent": "pentester",
  "depends_on": [],
  "need_env": false,
  "need_code": false,
  "risk_level": "low",
  "auth_required": false,
  "inputs": "对 192.168.1.0/24 进行全端口扫描",
  "success_criteria": "获得目标开放端口和服务版本列表",
  "evidence_requirements": ["nmap 扫描结果"],
  "status": "pending"
}
```

#### 代码复用来源

**Go 端 — 改造 `controller/subtasks.go`（现有约 200 行，改动约 40 行）**：

Planner 是**改造最深的节点**，因为它的两个模式（生成/调整）分别对应原项目的两套代码。

| 复用来源 | 文件 | 函数 | 复用方式 |
|---------|------|------|---------|
| 生成逻辑 | `controller/subtasks.go:70` `GenerateSubtasks()` | LLM 调用链 + DB 写入 | **改造**——改名为 `GenerateTodoPlan()`，改 prompt 和输出格式 |
| 调整逻辑 | `controller/subtasks.go:100` `RefineSubtasks()` | 获取 completed/planned → LLM 调整 | **改造**——改名为 `RefineTodoPlan()`，改 prompt 和输出格式 |
| Handler | `handlers.go` `GetGeneratorHandler()` / `GetRefinerHandler()` | 两步走模式 | **保留复用**——Planner 内部根据模式调用对应 handler |
| Performer | `performers.go` `performGenerator()` / `performRefiner()` | chain 管理 + LLM 调用 | **保留复用**——改 MsgChainType 为 `MsgchainTypePlanner` |
| Executor | `executor.go` `GetGeneratorExecutor()` / `GetRefinerExecutor()` | 工具集配置 | **保留复用**——工具从 subtask_list/subtask_patch 改为 todo_list/todo_patch |
| 消息链管理 | `helpers.go` `restoreChain()` | chain 恢复/创建 | **直接复用**——不改 |
| LLM 调用链 | `performer.go` `callWithRetries()` | 重试机制 | **直接复用**——不改 |

**关键改造点**：
- `subtask_list` → `todo_list`：输出格式从 `[{title, description}]` 改为完整 Todo 合同
- `subtask_patch` → `todo_patch`：delta 操作保持 add/remove/modify/reorder 不变
- 输入：从 `<user_task><input>` 改为 scope_contract JSON
- DB 写入：从 `subtasks` 表改为 `todos` 表

**Prompt — 改造现有模板（保留核心逻辑）**：

| 模式 | 原文件 | 改造方式 |
|------|--------|---------|
| 生成 | `generator.tmpl` | **保留**任务分解策略（10%→30%→30%→30%）、最小化步骤、不超过 N 个。**改**输入为 scope_contract，输出为完整 Todo 合同 |
| 生成输入 | `subtasks_generator.tmpl` | **改**输入模板：从 `<user_task><input>` 改为 scope_contract JSON |
| 调整 | `refiner.tmpl` | **保留**动态调整能力（add/remove/modify/reorder）、2 次失败后转向。**改**输出为 todo_patch |
| 调整输入 | `subtasks_refiner.tmpl` | **改**输入模板：从 `<planned_subtasks>` 改为 `<remaining_todos>` |

**新增代码量**：
- Go 端：`controller/subtasks.go` 改动 ~40 行 + `registry.go` 新增 ~20 行
- Prompt：4 个模板各改 ~30 行（共 ~120 行改动）

---

### 3.3 Supervisor — LLM 智能调度

**执行时机**：Planner 完成后持续循环，直到所有 Todo 完成并审查通过。

**职责**：读上下文 → 判断下一步 → 路由到目标 Agent。不执行任何业务操作。

**调度策略（两层）**：

```
第一层：结构化快速路径（不调 LLM，毫秒级）
  ├── scope_contract 缺失 → designer
  ├── plan 缺失 → planner
  └── Reporter 完成 → END

第二层：LLM 慢路径（调 LLM，秒级）
  ├── 执行节点返回了结果，需要判断"够不够好"
  ├── Reviewer 退回，需要理解退回原因并路由
  ├── Pentester 多轮执行，需要判断"是否继续"
  └── 异常/超时，需要判断"重试还是放弃"
```

**调用 Go 端**：
- Python 调 `_go_post("tasks/{id}/supervisor-step")`
- Go 端组装 `<supervisor_context>`，调 LLM，LLM 通过 `route_to_*` 工具声明路由决策

**supervisor_context 包含**：
- scope_contract
- plan（Todo 列表 + 状态）
- active_todo_id
- last_node_result（上一个节点返回的结果摘要）
- last_node（上一个执行的节点名称）

**路由工具**（全部是 Barrier 类型，Supervisor 只选一个）：
`route_to_designer`, `route_to_planner`, `route_to_builder`, `route_to_generator`, `route_to_integrator`, `route_to_tester`, `route_to_pentester`, `route_to_reviewer`, `route_to_reporter`, `route_to_auth_required`, `route_to_rejected`, `route_to_failed`, `route_to_end`

**消息链**：Supervisor 有独立 msgchain（`MsgchainTypeSupervisor`），与各执行节点 msgchain 隔离。

**防死循环**：连续 3 次路由到同一节点 + 结果相同 → 强制 `input_required`。

**状态变更与 Patch 合并流程**：
Supervisor 不仅做路由决策，还负责合并所有节点的 SharedAgentStatePatch：
1. 接收节点返回的 patch
2. 校验 patch 合法性（字段类型、枚举值）
3. 合并 patch 到 SharedAgentState（最小必要更新，不全量覆盖）
4. 写入 `status.json`（Workspace 断点恢复用）
5. 写入数据库 `shared_state` 字段
6. 输出 StreamChunk 事件（推送给前端）
7. 继续 `[agent-state]` 日志写入（审计和回放基础）

**Supervisor 自身输出（SharedAgentStatePatch）**：
- `decision`: 路由决策摘要（target + reason + todo_id）
- `agent_status`: 更新当前节点的状态为 COMPLETED，下一节点为 WORKING
- `history`: 追加上一轮的消息

#### 代码复用来源

**Go 端 — 改造 `orchestrator.go` 中 `DecidePrimaryAgentStep()`（现有约 200 行，改造约 80 行）**：

| 复用来源 | 文件 | 函数 | 复用方式 |
|---------|------|------|---------|
| 核心框架 | `orchestrator.go:57` `DecidePrimaryAgentStep()` | 消息链读取 → executor 构建 → LLM 调用 → 工具执行 | **改造**——复用完整调用链，改工具集和返回类型 |
| Barrier 处理 | `orchestrator.go` `barrierHandler` | 处理 ask 工具 | **直接复用**——ask 的 handler 不变 |
| Agent 委托 | `orchestrator.go` `buildAgentHandler()` | 委托 agent 执行 | **改造**——从"执行 agent"变为"声明路由目标" |
| 消息链管理 | `helpers.go` `restoreChain()` | chain 恢复/创建 | **直接复用**——改 MsgChainType 为 `MsgchainTypeSupervisor` |
| LLM 调用 | `performer.go` `callWithRetries()` | 重试机制 | **直接复用**——不改 |
| 上下文组装 | `provider.go` `prepareExecutionContext()` | 构建 `<execution_context>` | **改造**——新增 `GetSupervisorContext()` 组装 `<supervisor_context>` |

**关键改造对比**：

```
旧 DecidePrimaryAgentStep():
  工具集 = [pentester, coder, installer, searcher, memorist, adviser, done, ask]
  → LLM 调用 agent → decision.Action = "call_agent"
  → LLM 调用 done → decision.Action = "completed"

新 DecideSupervisorStep():
  工具集 = [route_to_designer, ..., route_to_auth_required, route_to_rejected, route_to_failed, route_to_end, ask]
  → LLM 调用 route_to_pentester → decision.Target = "pentester"
  → LLM 调用 route_to_auth_required → decision.Target = "auth_required"
  → LLM 调用 route_to_end → decision.Target = "end"
```

**Executor 工具集**：
```go
type SupervisorExecutorConfig struct {
    TaskID             *int64
    RouteToDesigner    tools.ExecutorHandler  // Barrier
    RouteToPlanner     tools.ExecutorHandler  // Barrier
    RouteToBuilder     tools.ExecutorHandler  // Barrier
    RouteToGenerator   tools.ExecutorHandler  // Barrier
    RouteToIntegrator  tools.ExecutorHandler  // Barrier
    RouteToTester      tools.ExecutorHandler  // Barrier
    RouteToPentester   tools.ExecutorHandler  // Barrier
    RouteToReviewer    tools.ExecutorHandler  // Barrier
    RouteToReporter    tools.ExecutorHandler  // Barrier
    RouteToEnd         tools.ExecutorHandler  // Barrier
    RouteToAuthRequired tools.ExecutorHandler // Barrier
    RouteToRejected    tools.ExecutorHandler  // Barrier
    RouteToFailed      tools.ExecutorHandler  // Barrier
    Ask                tools.ExecutorHandler  // Barrier
    Summarizer         tools.ExecutorHandler  // 内部
}
```

**Prompt — 新写 `prompts/supervisor.tmpl` + `prompts/question_supervisor.tmpl`（约 250 行）**：

| 参考来源 | 文件 | 参考内容 |
|---------|------|---------|
| 上下文装配 | `primary_agent.tmpl` | `<execution_context>` 解读方式——Supervisor 需要理解全局状态 |
| 路由决策规则 | `primary_agent.tmpl` | "最多 3 次重复"的死循环保护——直接继承 |
| 委托规则 | `primary_agent.tmpl` | "只在专家更适合时才委托"——改为"只选择最合适的下一个节点" |

**新增代码量**：
- Go 端：`orchestrator.go` 改造 ~80 行 + `executor.go` 新增 config ~55 行 + `registry.go` 新增 ~55 行 + `types.go` 新增 ~30 行
- Prompt：`supervisor.tmpl` ~180 行 + `question_supervisor.tmpl` ~70 行

---

### 3.4 Builder — 环境准备

**执行时机**：Supervisor 判断当前 Todo 的 `need_env=true` 且 `env_ready=false`。

**职责**：环境、镜像、依赖、工作目录、权限等准备工作。

**调用 Go 端**：
```python
_go_post("tasks/{id}/execute-agent", {
    "agent_type": "installer",  # Go 端仍用 installer handler
    "payload": {...},
})
```

#### 代码复用来源

**Go 端 — 零代码改动**：

| 复用来源 | 文件 | 函数 | 复用方式 |
|---------|------|------|---------|
| Handler | `handlers.go:344` `GetInstallerHandler()` | installerHandler + performInstaller | **完全复用**——Python 传 `agent_type="installer"` 直接走这条路径 |
| Executor | `executor.go` `GetInstallerExecutor()` | 工具集：terminal, file, adviser, memorist, searcher, maintenance_result | **完全复用**——不改 |
| 消息链 | `helpers.go` `restoreChain()` | MsgchainTypeInstaller | **完全复用**——不改 |
| LLM 调用 | `performer.go` `callWithRetries()` | 重试机制 | **完全复用**——不改 |

**Builder 就是 Installer**。Python 端的 `builder_node()` 只是名字变了，Go 端调用的还是 `installer` handler。

**状态变更输出（SharedAgentStatePatch）**：
- `message`: `role="Builder"`, `agent_status=COMPLETED/FAILED`
- `agent_status.builder`: `COMPLETED` 或 `FAILED`
- Todo 字段更新：`env_ready=true`（成功时）

**Prompt — 小改 `prompts/installer.tmpl`（+5 行）**：

| 保留（100%） | 新增 |
|-------------|------|
| "Elite DevOps engineer" 角色定义 | Workspace 目录结构初始化说明（5 行） |
| 安装前先检查是否已存在 | `mkdir -p artifact/raw artifact/final workspace` |
| 最多 2 次安装尝试 | |
| 失败时提供替代方案 | |
| 匿名化存储 | |
| terminal, file, search, store_guide, search_guide 工具 | |
| maintenance_result 输出 | |

**新增代码量**：
- Go 端：0 行
- Prompt：+5 行
- Python 端：`builder_node()` ~10 行（就是调 `_execute_agent("installer", state)`）

---

### 3.5 Generator — 代码生成

**执行时机**：Supervisor 判断当前 Todo 的 `need_code=true` 且 `code_generated=false`。

**职责**：生成 PoC、扫描脚本、验证代码。输出到 staging 区域，不直接写入最终路径。

**调用 Go 端**：
```python
_go_post("tasks/{id}/execute-agent", {
    "agent_type": "coder",  # Go 端仍用 coder handler
    "payload": {...},
})
```

#### 代码复用来源

**Go 端 — 零代码改动**：

| 复用来源 | 文件 | 函数 | 复用方式 |
|---------|------|------|---------|
| Handler | `handlers.go:247` `GetCoderHandler()` | coderHandler + performCoder | **完全复用**——Python 传 `agent_type="coder"` |
| Executor | `executor.go` `GetCoderExecutor()` | 工具集：terminal, file, adviser, installer, memorist, searcher, code_result | **完全复用**——不改 |
| 消息链 | `helpers.go` `restoreChain()` | MsgchainTypeCoder | **完全复用**——不改 |
| LLM 调用 | `performer.go` `callWithRetries()` | 重试机制 | **完全复用**——不改 |

**Generator 就是 Coder**。Python 端的 `generator_node()` 只是名字变了，Go 端调用的还是 `coder` handler。

**状态变更输出（SharedAgentStatePatch）**：
- `message`: `role="Generator"`, `agent_status=COMPLETED/FAILED`
- `agent_status.generator`: `COMPLETED` 或 `FAILED`
- Todo 字段更新：`code_generated=true`（成功时）
- 如有产物：注册 Artifact（代码文件 → `artifact/code/src/`）

**Prompt — 小改 `prompts/coder.tmpl`（+2 行）**：

| 保留（99%） | 新增 |
|------------|------|
| "Elite developer" 角色定义 | 输出路径改为 staging 区域：`/tmp/staging/`（1 行） |
| 代码质量要求 | 附加 metadata：language, dependencies, entry_command（1 行） |
| 终端执行规则 | |
| Graphiti → vector DB 记忆协议 | |
| 匿名化存储 | |
| terminal, file, search, graphiti_search, search_code, store_code 工具 | |
| code_result 输出 | |

**新增代码量**：
- Go 端：0 行
- Prompt：+2 行
- Python 端：`generator_node()` ~10 行（就是调 `_execute_agent("coder", state)`）

---

### 3.6 Integrator — 代码集成

**执行时机**：Generator 完成（`code_generated=true`），代码尚未集成（`code_integrated=false`）。

**职责**：把 Generator 产物从 staging 移到 Workspace 最终路径，创建 manifest.json，设置执行权限。不修改代码内容。

**调用 Go 端**：
```python
_go_post("tasks/{id}/execute-agent", {
    "agent_type": "integrator",
    "payload": {...},
})
```

#### 代码复用来源

**Go 端 — 新增 `providers/integrator.go`（约 100 行）**：

| 复用来源 | 文件 | 函数 | 复用方式 |
|---------|------|------|---------|
| Handler 模式 | `handlers.go:247` `coderHandler()` | 两步走（question + execute） | **复制模式**——改 prompt 类型为 `PromptTypeIntegrator` |
| Performer 模式 | `performers.go` `performCoder()` | restoreChain + performAgentChain | **复制模式**——改 MsgChainType 为 `MsgchainTypeIntegrator` |
| Executor 构建 | `executor.go` `GetCoderExecutor()` | 工具集配置 | **复制模式**——简化为只保留 terminal, file, integration_result, ask |
| 消息链管理 | `helpers.go` `restoreChain()` | chain 恢复/创建 | **直接复用**——不改 |
| LLM 调用 | `performer.go` `callWithRetries()` | 重试机制 | **直接复用**——不改 |

Integrator 的 executor 工具集是所有新 agent 中最少的（只有 terminal + file + integration_result + ask），因为它不涉及搜索、记忆、顾问。

**状态变更输出（SharedAgentStatePatch）**：
- `message`: `role="Integrator"`, `agent_status=COMPLETED/FAILED`
- `agent_status.integrator`: `COMPLETED` 或 `FAILED`
- Todo 字段更新：`code_integrated=true`（成功时）
- 产物：注册 Artifact，更新 `manifest.json`

**Prompt — 新写 `prompts/integrator.tmpl` + `prompts/question_integrator.tmpl`（约 120 行）**：

| 参考来源 | 参考内容 |
|---------|---------|
| 无直接参考 | Integrator 逻辑简单：读 staging → 移到最终路径 → 写 manifest.json → chmod |

**新增代码量**：
- Go 端：`providers/integrator.go` ~100 行 + `executor.go` 新增 config ~30 行 + `registry.go` 新增 ~20 行
- Prompt：`integrator.tmpl` ~80 行 + `question_integrator.tmpl` ~40 行

---

### 3.7 Tester — 代码验证

**执行时机**：Integrator 完成（`code_integrated=true`），代码尚未验证（`code_verified=false`）。

**职责**：验证代码能跑起来。不做真实攻击，用安全参数验证。失败时区分是代码问题还是环境问题。

**调用 Go 端**：
```python
_go_post("tasks/{id}/execute-agent", {
    "agent_type": "tester",
    "payload": {...},
})
```

**输出**：
```json
{
  "verified": true/false,
  "issue": "code" | "env" | "",
  "output": "...",
  "error": "..."
}
```

- `issue="code"` → Supervisor 路由回 Generator
- `issue="env"` → Supervisor 路由回 Builder

**状态变更输出（SharedAgentStatePatch）**：
- `message`: `role="Tester"`, `agent_status=COMPLETED/FAILED`
- `agent_status.tester`: `COMPLETED` 或 `FAILED`
- Todo 字段更新：`code_verified=true`（成功时）/ `issue=code/env`（失败时）

#### 代码复用来源

**Go 端 — 新增 `providers/tester.go`（约 110 行）**：

| 复用来源 | 文件 | 函数 | 复用方式 |
|---------|------|------|---------|
| Handler 模式 | `handlers.go:247` `coderHandler()` | 两步走（question + execute） | **复制模式**——改 prompt 类型为 `PromptTypeTester` |
| Performer 模式 | `performers.go` `performCoder()` | restoreChain + performAgentChain | **复制模式**——改 MsgChainType 为 `MsgchainTypeTester` |
| Executor 构建 | `executor.go` `GetCoderExecutor()` | 工具集配置 | **复制模式**——简化为 terminal, file, test_result, ask |
| 错误分析参考 | `handlers.go` `GetToolCallFixerHandler()` | 修复链模式 | **参考**——Tester 需要类似的错误分析能力 |
| 消息链管理 | `helpers.go` `restoreChain()` | chain 恢复/创建 | **直接复用**——不改 |
| LLM 调用 | `performer.go` `callWithRetries()` | 重试机制 | **直接复用**——不改 |

**Prompt — 新写 `prompts/tester.tmpl` + `prompts/question_tester.tmpl`（约 150 行）**：

| 参考来源 | 文件 | 参考内容 |
|---------|------|---------|
| 错误分析 | `toolcall_fixer.tmpl` | "分析错误→定位原因→判断是代码还是环境"的模式 |
| 验证逻辑 | `coder.tmpl` | 终端执行规则（绝对路径、超时、3 次重试） |

**新增代码量**：
- Go 端：`providers/tester.go` ~110 行 + `executor.go` 新增 config ~30 行 + `registry.go` 新增 ~20 行
- Prompt：`tester.tmpl` ~100 行 + `question_tester.tmpl` ~50 行

---

### 3.8 Pentester — 攻击执行

**执行时机**：Supervisor 判断当前 Todo 的执行条件已满足（env_ready, code_verified 等）。

**职责**：核心攻击执行。**改为多轮循环模式** — Supervisor 反复调用，每轮 Pentester 读取进度、决定下一步动作、调工具、回写结果。

**调用 Go 端**：
```python
_go_post("tasks/{id}/execute-agent", {
    "agent_type": "pentester",
    "payload": {...},
})
```

**关键约束**：Pentester 不应被削弱。它必须保留终端、浏览器、文件、情报检索、证据回写等全部核心能力。

**多轮执行流程**：
```
Supervisor → Pentester → hack_result → Supervisor
                                        ├── success_criteria 未满足 → Pentester (下一轮)
                                        ├── AUTH_REQUIRED → auth_required → 用户授权 → Supervisor → Pentester
                                        ├── 执行完成 → 标记 Todo done
                                        └── 异常 → 判断重试或放弃
```

#### 代码复用来源

**Go 端 — 零代码改动（handler 层），小改（executor 层）**：

| 复用来源 | 文件 | 函数 | 复用方式 |
|---------|------|------|---------|
| Handler | `handlers.go:575` `GetPentesterHandler()` | pentesterHandler + performPentester | **完全复用**——Python 传 `agent_type="pentester"` |
| Executor | `executor.go` `GetPentesterExecutor()` | 工具集 | **小改**——新增 `auth_required` 工具到 PentesterExecutorConfig |
| Performer | `performers.go` `performPentester()` | chain 管理 + LLM 调用 | **完全复用**——MsgchainTypePentester 不变 |
| 消息链 | `helpers.go` `restoreChain()` | chain 恢复/创建 | **完全复用**——每次 Supervisor 调用 Pentester 时，restoreChain 会恢复之前的消息，Pentester 能看到自己上一轮的 hack_result |
| LLM 调用 | `performer.go` `callWithRetries()` | 重试机制 | **完全复用**——不改 |

**多轮机制说明**：原项目中 Pentester 已经是一次调用内多轮工具调用（调 LLM → 调 nmap → 调 LLM → 调 sqlmap → ... → hack_result）。新设计的"多轮"是 Supervisor 在外层循环：每次调用 Pentester 执行一轮，Pentester 产出一个中间 hack_result，Supervisor 判断是否需要继续。Pentester 的 **内部执行逻辑完全不变**。

**状态变更输出（SharedAgentStatePatch）**：
- `message`: `role="Pentester"`, `agent_status=COMPLETED/FAILED/WORKING`
- `agent_status.pentester`: `COMPLETED`（本轮完成）/ `WORKING`（还需继续）
- Todo 字段更新：findings、evidence 追加
- 高风险操作时：返回 `auth_required` 标志，触发 AUTH_REQUIRED 流程

**AUTH_REQUIRED 流程**（Pentester 独有）：
```json
{
  "task_id": "...",
  "context_id": "...",
  "todo_id": "todo_003",
  "action": "执行 SQL 注入攻击",
  "risk_level": "high",
  "justification": "目标系统授权范围内的高风险操作",
  "status": "pending"
}
```
- task status 设为 `AUTH_REQUIRED` → 写入 `auth_requests` 表 → 写入 `memory/checkpoints/`
- 用户批准后：auth_requests.status="approved" → task status 回到 `WORKING` → Supervisor 路由回 Pentester 继续

**Prompt — 小改 `prompts/pentester.tmpl`（+15 行）**：

| 保留（95%） | 新增 |
|------------|------|
| "Advanced Penetration Testing Specialist" 角色定义 | 多轮循环模式说明（~10 行） |
| 完整渗透测试工具列表（50+ 工具） | 高风险操作 → `auth_required` 触发条件（~5 行） |
| MSFconsole 规则 | |
| 每次命令前检查端口 | |
| 最多 3 次相同工具重试 | |
| 存储成功方法到长期记忆 | |
| terminal, file, browser, hack_result 等全部工具 | |
| Graphiti + Guide 存储 | |

**新增代码量**：
- Go 端：`executor.go` 改动 ~5 行（加 auth_required 到 config）+ `registry.go` 新增 ~15 行
- Prompt：+15 行
- Python 端：`pentester_node()` ~15 行

---

### 3.9 Reviewer — 质量审查

**执行时机**：Supervisor 判断所有 Todo 已完成。

**职责**：审查结果的充分性、授权合规、证据链、成功标准、安全遗漏。不是"文笔审查"，是质量与合规总闸门。

**调用 Go 端**：
```python
_go_post("tasks/{id}/execute-agent", {
    "agent_type": "reviewer",
    "payload": {...},
})
```

**输入**：scope_contract + plan（完成状态）+ findings + evidence

**输出**：
```json
{
  "verdict": "PASS" | "FAIL" | "PARTIAL",
  "passed_criteria": [...],
  "failed_criteria": [...],
  "reject_to": "pentester" | "generator" | "builder" | "planner" | "designer",
  "reject_reason": "...",
  "recommendations": [...]
}
```

- PASS → Reporter
- FAIL → Supervisor 路由到 `reject_to` 指定的 Agent 修复

**状态变更输出（SharedAgentStatePatch）**：
- `message`: `role="Reviewer"`, `agent_status=COMPLETED`
- `agent_status.reviewer`: `COMPLETED`
- 产物：审查结论 Artifact，注册到 `manifest.json`

#### 代码复用来源

**Go 端 — 新增 `providers/reviewer.go`（约 120 行）**：

| 复用来源 | 文件 | 函数 | 复用方式 |
|---------|------|------|---------|
| Handler 模式 | `handlers.go:438` `memoristHandler()` | 两步走（question + execute） | **复制模式**——Reviewer 跟 memorist 类似（只读 + 输出结论） |
| Performer 模式 | `performers.go` `performMemorist()` | restoreChain + performAgentChain | **复制模式**——改 MsgChainType 为 `MsgchainTypeReviewer` |
| Executor 构建 | `executor.go` `GetMemoristExecutor()` | 工具集配置 | **复制模式**——改工具为 review_result + search_guide + graphiti_search + ask |
| 记忆检索 | `handlers.go` `GetMemoristHandler()` | 记忆能力 | **直接调用**——Reviewer 可查历史经验做对比 |
| 消息链管理 | `helpers.go` `restoreChain()` | chain 恢复/创建 | **直接复用**——不改 |
| LLM 调用 | `performer.go` `callWithRetries()` | 重试机制 | **直接复用**——不改 |

**Prompt — 新写 `prompts/reviewer.tmpl` + `prompts/question_reviewer.tmpl`（约 200 行）**：

| 参考来源 | 文件 | 参考内容 |
|---------|------|---------|
| 评估框架 | `reporter.tmpl` | "实际结果 vs 用户需求"的对比逻辑——Reviewer 继承这个核心模式 |
| 分析视角 | `adviser.tmpl` | "分析+建议"模式——Reviewer 的退回原因需要类似的分析能力 |
| 输入数据 | `subtasks_refiner.tmpl` | XML 输入模板——Reviewer 需要类似的执行日志输入 |

**新增代码量**：
- Go 端：`providers/reviewer.go` ~120 行 + `executor.go` 新增 config ~35 行 + `registry.go` 新增 ~20 行
- Prompt：`reviewer.tmpl` ~140 行 + `question_reviewer.tmpl` ~60 行

---

### 3.10 Reporter — 报告输出

**执行时机**：Reviewer PASS 后。

**职责**：基于已审查通过的结果出报告。不补事实，不推断证据。

**调用 Go 端**：
```python
_go_post("tasks/{id}/execute-agent", {
    "agent_type": "reporter",
    "payload": {...},
})
```

#### 代码复用来源

**Go 端 — 零代码改动**：

| 复用来源 | 文件 | 函数 | 复用方式 |
|---------|------|------|---------|
| Handler | `handlers.go` `GetReporterHandler()` | Reporter handler | **完全复用**——Python 传 `agent_type="reporter"` |
| Executor | `executor.go` `GetReporterExecutor()` | 工具集：report_result | **完全复用**——不改 |
| Performer | `performers.go` `performReporter()` | chain 管理 + LLM 调用 | **完全复用**——不改 |
| 消息链 | `helpers.go` `restoreChain()` | MsgchainTypeReporter | **完全复用**——不改 |

**Reporter 就是原项目的 Reporter**。逻辑完全不变。

**输入**：scope_contract + Reviewer 审查结论 + manifest + findings + evidence + artifacts + `[agent-state]` 事件摘要

**输出**：
- `artifact/final/report.md` — 最终渗透测试报告
- `artifact/final/report.json` — 结构化报告数据
- manifest 记录 `producer_agent="Reporter"`
- `message.role="Reporter"`, `response_type="artifact"`

**状态变更输出（SharedAgentStatePatch）**：
- `message`: `role="Reporter"`, `agent_status=COMPLETED`
- `agent_status.reporter`: `COMPLETED`
- 产物：最终报告 Artifact，注册到 manifest

**约束**：
- 不执行渗透测试动作
- 不修改代码产物
- 不绕过 Reviewer 的审查结论
- 不直接推进全局状态，只返回 SharedAgentStatePatch

**新增代码量**：
- Go 端：0 行
- Prompt：+3 行
- Python 端：`reporter_node()` ~10 行

---

### 3.11 辅助节点 — input_required / auth_required / rejected / failed

这四个节点不在主执行路径上，由 Supervisor 路由触发，负责处理中断和异常状态。

#### input_required — 等待用户输入

**触发条件**：Designer 信息不完整 / Supervisor 判断需要用户确认。

**行为**：
- task status 设为 `TASK_STATE_INPUT_REQUIRED`（6）
- 输出 StreamChunk 通知前端等待用户输入
- 用户回复后 → 回到 Designer（补充信息）或 Supervisor（继续执行）

**状态变更输出**：
- `decision`: "waiting for user input"
- `agent_status`: 不更新（保留各角色当前状态）

#### auth_required — 等待授权

**触发条件**：Pentester 高风险操作 / Designer 发现授权范围不明确。

**行为**：
- task status 设为 `TASK_STATE_AUTH_REQUIRED`（8）
- 创建 `auth_requests` 记录（pending 状态）
- 写入 `memory/checkpoints/` 保存当前进度
- 输出 StreamChunk 通知前端等待授权
- 用户批准 → auth_requests.status="approved" → task status 回到 `WORKING` → Supervisor 继续
- 用户拒绝 → auth_requests.status="rejected" → Supervisor 路由到 `rejected`

#### rejected — 策略拒绝

**触发条件**：用户拒绝授权 / 目标超出 authorized_scope / 安全策略阻止。

**行为**：
- task status 设为 `TASK_STATE_REJECTED`（7）— 终态
- 输出 StreamChunk（`is_final=True`）
- → END

#### failed — 不可恢复错误

**触发条件**：结构化错误且 `retryable=false` / 超过最大重试次数 / 系统级错误。

**行为**：
- task status 设为 `TASK_STATE_FAILED`（4）— 终态
- 输出 StructuredError（含 error_code, node, timestamp）
- 输出 StreamChunk（`is_final=True`）
- → END

---

## 4. 共享服务层（不改动）

以下能力不作为编排节点，而是被各主角色通过 **executor config 中嵌入 handler** 调用。**代码完全不改**。

### 调用机制

共享服务不是独立的编排节点，而是在各主角色的 executor config 中注册为 handler。LLM 在执行主角色任务时可以自主决定调用这些子能力。

例如 PentesterExecutorConfig 包含 Searcher handler，Pentester 的 LLM 可以在执行渗透测试时调用 searcher 获取情报——这跟原项目的模式完全一样。

```
主角色 ExecutorConfig
├── 主角色专用工具（barrier）
├── Searcher handler（可选）
├── Memorist handler（可选）
├── Adviser handler（可选）
└── Summarizer handler（内部）
```

### 各共享服务详情

| 服务 | Go 端代码 | Prompt | 调用方 |
|------|---------|--------|--------|
| **Searcher** | `handlers.go:674` `GetSubtaskSearcherHandler()` → `performSearcher()` → `GetSearcherExecutor()` | `searcher.tmpl` + `question_searcher.tmpl` | Designer, Planner, Pentester |
| **Enricher** | `handlers.go` `GetEnricherHandler()` → `performEnricher()` → `GetEnricherExecutor()` | `enricher.tmpl` + `question_enricher.tmpl` | Designer, Planner |
| **Adviser** | `handlers.go:72` `GetAskAdviceHandler()` → `performSimpleChain()` | `adviser.tmpl` + `question_adviser.tmpl` | Planner, Pentester, Reviewer |
| **Memorist** | `handlers.go:438` `GetMemoristHandler()` → `performMemorist()` → `GetMemoristExecutor()` | `memorist.tmpl` + `question_memorist.tmpl` | Planner, Pentester, Reviewer |
| **Summarizer** | 内部调用（长上下文管理） | `summarizer.tmpl` | 所有主角色 |
| **Reflector** | 内部调用（高保真摘要） | `reflector.tmpl` + `question_reflector.tmpl` | 所有主角色 |
| **ToolCallFixer** | `handlers.go` `GetToolCallFixerHandler()` | `toolcall_fixer.tmpl` + `input_toolcall_fixer.tmpl` | Generator, Tester, Pentester |
| **Assistant** | 独立入口，不入主编排链 | `assistant.tmpl` | 旧对话模式保留 |

---

## 5. 代码改造清单

> 核心原则：**框架代码 100% 复用，核心 agent 代码 95% 保留，编排层直接替换**。

### 5.1 改动量总览

| 类别 | 文件数 | 详情 |
|------|--------|------|
| **完全不改** | ~20 个文件 | 共享服务层全部 handler/performer/executor/prompt |
| **完全复用（零改动）** | 3 个 agent | Builder(installer), Generator(coder), Reporter — Go 端零代码改动 |
| **小改（<20 行）** | 4 个 prompt | installer.tmpl(+5), coder.tmpl(+2), pentester.tmpl(+15), reporter.tmpl(+3) |
| **新增 Go 文件** | 4 个 | providers/designer.go, integrator.go, tester.go, reviewer.go（各 ~100 行） |
| **新增 Prompt** | 12 个 | 6 个新 agent 各 2 个模板（question + system） |
| **改造 Go 文件** | 3 个 | orchestrator.go(改造~80行), executor.go(新增~200行), registry.go(新增~180行) |
| **重写 Python** | 1 个 | orchestrator/app.py |
| **新增类型** | 1 个 | types.go 新增 SupervisorDecision, StructuredError, AgentRole |
| **数据库迁移** | 1 个 | 新建 todos, findings, evidence, artifacts, auth_requests 表；tasks 表新增 context_id 等字段 |

### 5.2 orchestrator/app.py — 直接重写

**删除**：
- `generate_subtasks()`, `select_next_subtask()`, `prepare_primary_agent_context()`, `primary_agent()`
- `refine_subtasks()`
- `route_after_select_next_subtask()`, `route_after_primary_agent()`
- `_execute_agent_node()` 工厂函数（改为通用 `_execute_agent()` + 各节点独立函数）
- 旧的图拓扑定义

**新增**：
- `designer()` — 调 Go 端 designer-step
- `planner()` — 调 Go 端 generate-todo-plan / refine-todo-plan
- `supervisor()` — 先走快速路径，不行调 Go 端 supervisor-step
- `builder_node()` — 调 `_execute_agent("installer", state)`
- `generator_node()` — 调 `_execute_agent("coder", state)`
- `integrator_node()` — 调 `_execute_agent("integrator", state)`
- `tester_node()` — 调 `_execute_agent("tester", state)`
- `pentester_node()` — 调 `_execute_agent("pentester", state)`
- `reviewer_node()` — 调 `_execute_agent("reviewer", state)`
- `reporter_node()` — 调 `_execute_agent("reporter", state)`
- `auth_required()` — 授权中断处理
- `rejected()` — 策略拒绝处理
- `failed()` — 不可恢复错误处理
- `route_supervisor()`, `route_after_designer()`, `route_after_reviewer()` — 路由函数
- `_execute_agent()` — 提取的通用 agent 执行函数（复用旧 `_execute_agent_node()` 逻辑）
- `_supervisor_fast_path()` — 结构化快速路径（不调 LLM）
- `_merge_state_patch()` — 合并 SharedAgentStatePatch
- 新图拓扑定义

**保留不变**：
- `_go_post()` 通信层
- `input_required()` 节点
- TaskState 定义（扩展）
- checkpoint / thread 管理

---

### 5.3 providers/orchestrator.go — 改造

**改造 `DecidePrimaryAgentStep()` → `DecideSupervisorStep()`**：

改造范围约 80 行。复用完整的调用链基础设施，只改工具集和返回类型。

| 改造点 | 旧 | 新 |
|--------|-----|-----|
| 工具集 | coder/pentester/searcher/installer/memorist/adviser + done + ask | 11 个 route_to_* + ask |
| 上下文 | `getExecutionContext()` → `<execution_context>` | `GetSupervisorContext()` → `<supervisor_context>` |
| 消息链类型 | MsgchainTypePrimaryAgent | MsgchainTypeSupervisor |
| 返回类型 | `PrimaryAgentDecision{Action, AgentType, Result}` | `SupervisorDecision{Target, TodoID, Reason}` |
| Barrier | done(success=true → completed, false → failed) + ask | route_to_end → Target="end", 其余 route_to_* → Target=对应节点 |

**保留 `ExecuteDelegatedAgent()`** — 核心执行逻辑不变，新增 agentType 映射：
```go
// 新增映射
"integrator" → GetIntegratorHandler()
"tester"     → GetTesterHandler()
"reviewer"   → GetReviewerHandler()

// 保留映射
"coder"      → GetCoderHandler()        // Generator 复用
"pentester"  → GetPentesterHandler()     // 直接复用
"installer"  → GetInstallerHandler()     // Builder 复用
"searcher"   → GetSubtaskSearcherHandler()
"memorist"   → GetMemoristHandler()
"adviser"    → GetAskAdviceHandler()
```

---

### 5.4 新增 Go 端文件

每个新文件都复用相同的四层模式（handler → performer → executor → registry），代码量约 100 行。

| 文件 | 行数 | 复制的模板函数 | 改动点 |
|------|------|--------------|--------|
| `providers/designer.go` | ~120 | 复制 `searcherHandler()` + `performSearcher()` | 改 prompt 类型、MsgChainType、executor config |
| `providers/integrator.go` | ~100 | 复制 `coderHandler()` + `performCoder()` | 简化工具集（只保留 terminal/file/integration_result/ask） |
| `providers/tester.go` | ~110 | 复制 `coderHandler()` + `performCoder()` | 简化工具集（只保留 terminal/file/test_result/ask） |
| `providers/reviewer.go` | ~120 | 复制 `memoristHandler()` + `performMemorist()` | 改工具集（review_result + search_guide + graphiti_search + ask） |

---

### 5.5 executor.go — 新增 5 个 ExecutorConfig

在现有 `Get*Executor()` 方法旁边新增，不改动现有的任何 executor config。

| 新增 Config | 行数 | 参考模板 | 工具集 |
|-------------|------|---------|--------|
| `SupervisorExecutorConfig` | ~55 | `PrimaryExecutorConfig` | 13 个 route_to_* + ask + summarizer |
| `DesignerExecutorConfig` | ~35 | `SearcherExecutorConfig` | scope_contract + searcher + memorist + ask + summarizer |
| `IntegratorExecutorConfig` | ~30 | `CoderExecutorConfig`（简化） | terminal + file + integration_result + ask |
| `TesterExecutorConfig` | ~30 | `CoderExecutorConfig`（简化） | terminal + file + test_result + ask |
| `ReviewerExecutorConfig` | ~40 | `MemoristExecutorConfig` | review_result + memorist + graphiti_search + ask |

**保留不改**：GetPrimaryExecutor, GetCoderExecutor, GetPentesterExecutor, GetInstallerExecutor, GetSearcherExecutor, GetMemoristExecutor, GetGeneratorExecutor, GetRefinerExecutor, GetReporterExecutor, GetEnricherExecutor

---

### 5.6 tools/registry.go — 新增工具定义

新增约 21 个工具常量和对应的 `llms.FunctionDefinition`（schema 定义）。现有的 37 个工具常量全部保留不改。

```go
// 新增工具名（约 21 个）
const (
    // 新 Agent 专用输出工具
    ScopeContractToolName     = "scope_contract"
    TodoListToolName          = "todo_list"
    TodoPatchToolName         = "todo_patch"
    IntegrationResultToolName = "integration_result"
    TestResultToolName        = "test_result"
    ReviewResultToolName      = "review_result"
    AuthRequiredToolName      = "auth_required"
    SharedStatePatchToolName  = "shared_state_patch"
    ArtifactToolName          = "artifact"
    // Supervisor 路由工具（13 个）
    RouteToDesignerToolName      = "route_to_designer"
    RouteToPlannerToolName       = "route_to_planner"
    RouteToBuilderToolName       = "route_to_builder"
    RouteToGeneratorToolName     = "route_to_generator"
    RouteToIntegratorToolName    = "route_to_integrator"
    RouteToTesterToolName        = "route_to_tester"
    RouteToPentesterToolName     = "route_to_pentester"
    RouteToReviewerToolName      = "route_to_reviewer"
    RouteToReporterToolName      = "route_to_reporter"
    RouteToAuthRequiredToolName  = "route_to_auth_required"
    RouteToRejectedToolName      = "route_to_rejected"
    RouteToFailedToolName        = "route_to_failed"
    RouteToEndToolName           = "route_to_end"
)
```

类型映射新增：
```go
// StoreAgentResultToolType
ScopeContractToolName, TodoListToolName, TodoPatchToolName,
IntegrationResultToolName, TestResultToolName, ReviewResultToolName,
SharedStatePatchToolName, ArtifactToolName

// BarrierToolType
AuthRequiredToolName, RouteTo*ToolName (全部 13 个)
```

---

### 5.7 types.go — 新增类型

```go
// Supervisor 路由决策
type SupervisorDecision struct {
    Target      string `json:"target"`       // designer/planner/builder/generator/integrator/tester/pentester/reviewer/reporter/auth_required/rejected/failed/end
    TodoID      string `json:"todo_id"`
    Reason      string `json:"reason"`
    Retryable   bool   `json:"retryable"`
    MsgChainID  int64  `json:"msg_chain_id"`
}

// 结构化错误
type StructuredError struct {
    ErrorCode    string `json:"error_code"`
    ErrorMessage string `json:"error_message"`
    TaskID       string `json:"task_id"`
    Node         string `json:"node"`
    Retryable    bool   `json:"retryable"`
    Timestamp    string `json:"timestamp"`
}

// AgentRole 枚举（Go mirror）
type AgentRole string
const (
    AgentRoleUser       AgentRole = "User"
    AgentRoleSupervisor AgentRole = "Supervisor"
    AgentRoleDesigner   AgentRole = "Designer"
    AgentRolePlanner    AgentRole = "Planner"
    AgentRoleGenerator  AgentRole = "Generator"
    AgentRoleIntegrator AgentRole = "Integrator"
    AgentRoleReviewer   AgentRole = "Reviewer"
    AgentRoleBuilder    AgentRole = "Builder"
    AgentRoleTester     AgentRole = "Tester"
    AgentRolePentester  AgentRole = "Pentester"
    AgentRoleReporter   AgentRole = "Reporter"
)

// 保留 PrimaryAgentDecision（可后续重命名为 AgentDecision）
// 保留 AgentExecutionResult（完全不变）
```

---

### 5.8 controller/ — 改造

`controller/subtasks.go`：改造 `GenerateSubtasks()` 和 `RefineSubtasks()`。

| 旧函数 | 新函数 | 改动点 |
|--------|--------|--------|
| `GenerateSubtasks()` | `GenerateTodoPlan()` | prompt 用 planner.tmpl，输出用 todo_list，DB 写入 todos 表 |
| `RefineSubtasks()` | `RefineTodoPlan()` | prompt 用 planner.tmpl(调整模式)，输出用 todo_patch |

`controller/subtask.go`：SubtaskWorker 接口保持不变，内部 `StepPrimaryAgent()` 改为调用 `DecideSupervisorStep()`。

---

### 5.9 数据库迁移

```sql
-- tasks 表新增字段
ALTER TABLE tasks ADD COLUMN scope_contract JSONB;
ALTER TABLE tasks ADD COLUMN context_id VARCHAR(128);        -- Workspace 和消息隔离
ALTER TABLE tasks ADD COLUMN state_id VARCHAR(128);          -- 状态版本追踪，乐观锁
ALTER TABLE tasks ADD COLUMN protocol_version VARCHAR(32) DEFAULT 'multi-agent-v1.0';
ALTER TABLE tasks ADD COLUMN shared_state JSONB;             -- SharedAgentState 快照
ALTER TABLE tasks ADD COLUMN task_status_code INT DEFAULT 0; -- TaskStateEnum
ALTER TABLE tasks ADD COLUMN normalized_state VARCHAR(32) DEFAULT 'SUBMITTED';
ALTER TABLE tasks ADD COLUMN active_node VARCHAR(64);
ALTER TABLE tasks ADD COLUMN active_todo_id VARCHAR(128);

-- 新建 todos 表（替代 subtasks）
CREATE TABLE todos (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    todo_id VARCHAR(128) NOT NULL,
    title TEXT NOT NULL,
    owner_agent VARCHAR(64) NOT NULL,
    depends_on JSONB DEFAULT '[]',
    need_env BOOLEAN DEFAULT FALSE,
    need_code BOOLEAN DEFAULT FALSE,
    risk_level VARCHAR(16) DEFAULT 'low',
    auth_required BOOLEAN DEFAULT FALSE,
    inputs TEXT,
    success_criteria TEXT,
    evidence_requirements JSONB DEFAULT '[]',
    data JSONB,                    -- Todo 扩展字段（PentAGI 特有）
    todo_status_code INT DEFAULT 0,  -- Todo 级别状态码（0=pending, 1=ready, 2=running, 3=done, 4=failed, 5=skipped, 6=blocked）
    status VARCHAR(32) DEFAULT 'pending',
    result TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(task_id, todo_id)
);

-- artifacts 表（产物追踪）
CREATE TABLE artifacts (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    artifact_id VARCHAR(128) NOT NULL,
    name TEXT NOT NULL,
    artifact_type VARCHAR(16) NOT NULL,  -- text / dir
    relative_path TEXT,
    description TEXT,
    producer_agent VARCHAR(64),
    version VARCHAR(32),
    checksum VARCHAR(128),
    text TEXT,
    code_status JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(task_id, artifact_id)
);

-- auth_requests 表（授权追踪）
CREATE TABLE auth_requests (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    context_id VARCHAR(128) NOT NULL,
    todo_id VARCHAR(128),
    action TEXT NOT NULL,
    risk_level VARCHAR(16) NOT NULL,     -- low / medium / high / critical
    justification TEXT NOT NULL,
    status VARCHAR(32) DEFAULT 'pending', -- pending / approved / rejected
    response TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    resolved_at TIMESTAMP WITH TIME ZONE
);

-- findings 表（Pentester 发现）
CREATE TABLE findings (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    todo_id VARCHAR(128),
    finding_type VARCHAR(64),
    severity VARCHAR(16),
    title TEXT NOT NULL,
    description TEXT,
    evidence JSONB DEFAULT '[]',
    raw_output TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- evidence 表（证据链）
CREATE TABLE evidence (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    todo_id VARCHAR(128),
    artifact_id VARCHAR(128),
    evidence_type VARCHAR(64),
    relative_path TEXT,
    description TEXT,
    hash VARCHAR(128),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- msgchain enum 扩展（新增角色必须进入数据库 enum）
ALTER TABLE msg_chains ALTER COLUMN type TYPE VARCHAR(64);
-- 新增枚举值：supervisor, designer, planner, integrator, reviewer, tester
-- 保留现有值：coder, pentester, installer, searcher, memorist, adviser, reporter, enricher, generator, refiner, tool_call_fixer

-- msglogs 表补充（如果 context_id 列不存在）
ALTER TABLE msglogs ADD COLUMN IF NOT EXISTS context_id VARCHAR(128);
```

---

### 5.10 API 端点变更

```go
// 新增端点
POST /internal/orchestrator/tasks/{id}/designer-step
POST /internal/orchestrator/tasks/{id}/supervisor-step
POST /internal/orchestrator/tasks/{id}/generate-todo-plan
POST /internal/orchestrator/tasks/{id}/refine-todo-plan
POST /internal/orchestrator/tasks/{id}/merge-state-patch    // 合并 SharedAgentStatePatch
POST /internal/orchestrator/tasks/{id}/stream-event         // 流式事件输出
POST /internal/orchestrator/tasks/{id}/store-artifact       // 产物存储
POST /internal/orchestrator/tasks/{id}/store-auth-request   // 授权请求
POST /internal/orchestrator/tasks/{id}/reject-task          // 策略拒绝
POST /internal/orchestrator/tasks/{id}/complete-task        // 任务完成

// 保留的端点（逻辑不变）
POST /internal/orchestrator/tasks/{id}/execute-agent
POST /internal/orchestrator/tasks/{id}/report-task-result
POST /internal/orchestrator/tasks/{id}/fail-task

// 删除的端点
// generate-subtasks, select-next-subtask, primary-agent-step,
// write-primary-agent-result, refine-subtasks
```

---

## 6. TaskState 定义（LangGraph 图状态）

> **注意**：Section 6 的 `TaskState`（TypedDict）是 LangGraph `StateGraph` 的图状态定义，用于 Python 编排层。
> Section 7 的 `SharedAgentState`（Pydantic BaseModel）是规范要求的权威状态快照模型，用于持久化和审计。
> 两者的关系：`TaskState`（TypedDict）是 LangGraph 运行时的可变状态容器，每个节点执行完毕后，调度器将其中的关键字段同步到 `SharedAgentState`（Pydantic）并持久化。

```python
class TodoItem(TypedDict, total=False):
    todo_id: str
    title: str
    owner_agent: str
    depends_on: list[str]
    need_env: bool
    need_code: bool
    risk_level: str              # low/medium/high
    auth_required: bool
    inputs: str
    success_criteria: str
    evidence_requirements: list[str]
    status: str                  # pending/ready/running/blocked/done/failed/skipped
    # 运行时状态
    env_ready: bool
    code_generated: bool
    code_integrated: bool
    code_verified: bool


class TaskState(TypedDict, total=False):
    # 基础标识
    flow_id: int
    task_id: int

    # 状态（来自规范 2.6）
    normalized_state: str        # SUBMITTED/WORKING/INPUT_REQUIRED/AUTH_REQUIRED/COMPLETED/FAILED/CANCELED/REJECTED
    active_node: str             # 当前运行位置
    active_todo_id: str          # 当前 Todo

    # Supervisor 调度
    route_history: list[dict]    # 路由历史（防死循环）
    last_node: str               # 上一个节点
    last_node_success: bool      # 上一个节点是否成功

    # 业务数据
    scope_contract: dict         # Designer 输出
    plan: list[TodoItem]         # Planner 输出的 Todo 列表
    plan_needs_update: bool      # 是否需要 Planner 调整

    # 执行结果
    last_agent_result: str
    findings: list[dict]         # Pentester 的发现
    evidence: list[dict]         # 证据清单
    review_result: dict          # Reviewer 的审查结论
```

---

## 7. 规范状态模型

> 来源：队友方案 + 已有 agent-state-log 分支基础。以下模型在 TaskState 之上补充规范要求的结构化状态管理。

### 7.0 已有基础（agent-state-log 分支）

当前分支已在 `backend/pkg/controller/subtask.go` 中实现：

- `agentExecuteState`：覆盖 `AGENT_STATE_UNSPECIFIED / WORKING / COMPLETED / FAILED`
- `agentStateLogEntry`：包含 `schema_version / message_id / event / node / role / agent_status / task_id / context_id / subtask_id / timestamp / details`
- `[agent-state]` 日志前缀：状态事件已写入 `msglogs`
- `trimAgentStateValue` / `agentStateSecretPatterns`：敏感内容脱敏

改造策略：**不是另起一套，而是提升现有基础**：

```text
agentStateLogEntry      = 状态事件 / 审计日志 / 回放基础（已有）
AgentStateList          = 从最近状态事件聚合出的各角色当前状态（新增聚合层）
SharedAgentState        = 当前任务的权威共享状态快照（新增）
SharedAgentStatePatch   = 单个节点返回的状态增量（新增）
```

### 7.1 TaskState 整数枚举

在 Task 6 的 TypedDict 基础上，增加整数枚举用于 Go 端和数据库：

```python
class TaskStateEnum(int, Enum):
    TASK_STATE_UNSPECIFIED = 0
    TASK_STATE_SUBMITTED = 1
    TASK_STATE_WORKING = 2
    TASK_STATE_COMPLETED = 3
    TASK_STATE_FAILED = 4
    TASK_STATE_CANCELED = 5
    TASK_STATE_INPUT_REQUIRED = 6
    TASK_STATE_REJECTED = 7
    TASK_STATE_AUTH_REQUIRED = 8
```

约束：
- `COMPLETED / FAILED / CANCELED` 为终态，进入后不可回退
- `INPUT_REQUIRED / AUTH_REQUIRED` 为中断态，补全后可回到 `WORKING`
- `REJECTED` 用于策略拒绝（越权目标等），不等同于执行失败
- `task_status` 只能由调度器或后端统一更新

### 7.2 AgentExecuteState 枚举

复用并提升现有 `agentExecuteState`：

```python
class AgentExecuteState(int, Enum):
    AGENT_STATE_UNSPECIFIED = 0
    AGENT_STATE_WORKING = 1
    AGENT_STATE_COMPLETED = 2
    AGENT_STATE_FAILED = 3
```

Go 端：直接复用现有 `agentExecuteState` 定义，避免再定义第二套。

### 7.3 AgentStateList — 各角色状态追踪

规范角色字段固定，不运行期动态扩展：

```python
class AgentStateList(BaseModel):
    supervisor:  AgentExecuteState = AGENT_STATE_UNSPECIFIED
    designer:    AgentExecuteState = AGENT_STATE_UNSPECIFIED
    planner:     AgentExecuteState = AGENT_STATE_UNSPECIFIED
    generator:   AgentExecuteState = AGENT_STATE_UNSPECIFIED
    integrator:  AgentExecuteState = AGENT_STATE_UNSPECIFIED
    reviewer:    AgentExecuteState = AGENT_STATE_UNSPECIFIED
    builder:     AgentExecuteState = AGENT_STATE_UNSPECIFIED
    tester:      AgentExecuteState = AGENT_STATE_UNSPECIFIED
    pentester:   AgentExecuteState = AGENT_STATE_UNSPECIFIED
    reporter:    AgentExecuteState = AGENT_STATE_UNSPECIFIED
```

实现：从 `[agent-state]` append-only 事件聚合当前各角色状态。

### 7.4 SharedAgentState — 权威状态快照

```python
class SharedAgentState(BaseModel):
    state_id: str                              # 版本追踪，乐观锁依据
    task: Task                                 # 当前任务
    todos: list[Task] | None = None            # 任务拆分结果
    decision: str | None = None                # Supervisor 结构化决策摘要
    message: Message | None = None             # 最近消息
    history: list[Message] = []                # 消息历史（审计回放）
    code_status: CodeState | None = None       # 代码生命周期（仅代码类任务）
    agent_status: AgentStateList               # 各角色执行状态
```

### 7.5 SharedAgentStatePatch — 节点只返回增量

每个节点只能返回 patch，调度器负责合并：

```python
class SharedAgentStatePatch(BaseModel):
    todos: list[Task] | None = None
    decision: str | None = None
    message: Message | None = None
    history: list[Message] | None = None
    code_status: CodeState | None = None
    agent_status: AgentStateList | None = None
```

调度器职责：校验 patch → 合并 patch → 写入 `status.json` → 写入数据库 → 输出 stream event。

### 7.6 数据库 ↔ Pydantic 模型对应关系

| Pydantic 模型 | 数据库存储 | 说明 |
|-------------|----------|------|
| `SharedAgentState.state_id` | `tasks.state_id` | 状态版本，乐观锁 |
| `SharedAgentState.task` | `tasks` 表 | 任务基础信息 |
| `SharedAgentState.todos` | `todos` 表 | 每条 Todo 一行 |
| `SharedAgentState.decision` | `tasks.shared_state` JSONB | 决策摘要 |
| `SharedAgentState.message` | `msglogs` 表 | 最近消息 |
| `SharedAgentState.history` | `msglogs` 表 | 按时间排序 |
| `SharedAgentState.code_status` | `tasks.shared_state` JSONB | 代码生命周期 |
| `SharedAgentState.agent_status` | 从 `[agent-state]` 日志聚合 | 不存独立表 |
| `TaskStateEnum` | `tasks.task_status_code` INT | 任务总状态 |
| `TodoItem.status` | `todos.todo_status_code` INT | Todo 状态 |
| `Artifact` | `artifacts` 表 | 产物追踪 |
| `AuthRequest` | `auth_requests` 表 | 授权追踪 |
| `StructuredError` | `msglogs` + StreamChunk | 不存独立表 |
| `Message.context_id` | `msglogs.context_id` | 消息隔离 |
| `AgentRole` 枚举 | `msg_chains.type` | 消息链类型 |

---

## 8. Workspace 规范

> 来源：队友方案。后端创建任务时必须创建独立 Workspace。

### 8.1 目录结构

```text
~/.workspace/<task_id>/
  session.json
  README.md
  task.md
  status.json              # 最新结构化状态快照，用于断点恢复
  input/
    prompt.md
    files/
    refs/
    config/
  artifact/
    final/                  # 最终报告
    preview/
    manifest.json           # 产物索引
    code/
      src/
      docs/
      builds/
    tester/
      exports/
      report.md
    pentester/
      exploit/
      exports/
      tmp/
  memory/
    checkpoints/            # 流程阶段快照
    memory.md
  log/
    app.log
    roles/
      designer.log
      planner.log
      generator.log
      integrator.log
      reviewer.log
      builder.log
      tester.log
      pentester.log
      reporter.log
  cache/
  archive/
```

### 8.2 manifest.json

`artifact/manifest.json` 至少包含：

```json
{
  "artifact_id": "",
  "name": "",
  "type": "",
  "relative_path": "",
  "description": "",
  "created_at": "",
  "producer_agent": "",
  "version": "",
  "checksum": ""
}
```

约束：
- 所有产物必须注册 manifest
- 前端只通过后端接口读取 manifest 和产物，不直接访问 Workspace
- 智能体只允许在授权范围内读写当前 Workspace
- `input/` 原始输入只读，不覆盖

---

## 9. 消息、产物与流式协议

> 来源：队友方案。结构化通信协议，确保规范合规。

### 9.1 AgentRole 枚举

```python
AgentRole = Literal[
    "User", "Supervisor", "Designer", "Planner",
    "Generator", "Integrator", "Reviewer", "Builder",
    "Tester", "Pentester", "Reporter",
]
```

`Reporter` 必须作为枚举值，不能作为自由字符串。

### 9.2 Message

```python
class Message(BaseModel):
    message_id: str
    context_id: str
    task_id: str
    role: AgentRole
    message_type: MessageType
    agent_status: AgentExecuteState
    timestamp: datetime
```

### 9.3 Artifact

```python
class Artifact(BaseModel):
    task_id: str
    artifact_id: str
    name: str
    description: str | None = None
    artifact_type: Literal["text", "dir"]
    text: str | None = None
```

### 9.4 StreamChunk

```python
class StreamChunk(BaseModel):
    event: Literal["status", "updates", "error", "end"]
    updates: dict[str, Any] | None = None
    error: StructuredError | None = None
```

约束：每个 stream 最终必须输出 `end`。错误事件输出后，由调度器决定结束、重试、进入 `INPUT_REQUIRED` 或 `AUTH_REQUIRED`。

---

## 10. 结构化错误

> 来源：队友方案。所有错误返回必须结构化，不得使用自由文本。

```python
class StructuredError(BaseModel):
    error_code: str
    error_message: str
    task_id: str
    node: str
    retryable: bool
    timestamp: datetime
```

错误分类：

| 类别 | 说明 | 处理 |
|------|------|------|
| 参数错误 | 输入不合法 | `INPUT_REQUIRED` |
| 状态错误 | 操作顺序不对 | `FAILED` |
| 调度错误 | 路由/循环问题 | `FAILED` |
| 智能体执行错误 | LLM/工具失败 | `retryable=true` 时重试 |
| 文件系统错误 | Workspace 操作失败 | `FAILED` |
| 鉴权错误 | 授权缺失 | `AUTH_REQUIRED` |
| 外部依赖错误 | 搜索/记忆服务不可用 | `retryable=true` 时重试 |

---

## 11. CodeState 生命周期分析

> 辩证分析：队友方案将 Builder 放在 Reviewer 之后（DESIGNER→...→REVIEWER→BUILDER→TESTER），而本方案将 Builder 放在 Generator 之前（BUILDER→GENERATOR→INTEGRATOR→TESTER）。两者各有道理。

### 11.1 两种顺序对比

| 顺序 | 适用场景 | 优势 | 劣势 |
|------|---------|------|------|
| **Builder → Generator → Integrator → Tester** | 渗透测试 | 先准备环境（安装工具、配置网络），代码生成时有现成环境可测试 | 代码审查通过后可能需要重建环境 |
| **Generator → Integrator → Reviewer → Builder → Tester** | 纯代码开发 | 符合标准软件工程流程（写完代码→审查→构建→测试） | 渗透测试中环境准备太晚会拖延进度 |

### 11.2 本方案的选择

**保留 Builder 前置的顺序**（即本方案当前设计），理由：

1. **渗透测试的核心是环境**：没有 nmap/sqlmap/metasploit 等工具，代码生成了也没法验证
2. **Builder 是轻量操作**：在渗透测试中，Builder 通常只是安装依赖和初始化工作目录，不需要等到代码审查后
3. **Supervisor 灵活路由**：如果某些 Todo 确实需要"先写代码再构建"（如纯脚本任务），Supervisor 可以跳过 Builder 前置步骤

### 11.3 CodeImplementState（可选增强）

如果后续需要更细粒度的代码状态追踪，可引入 24 态枚举：

```python
class CodeImplementState(int, Enum):
    CODE_STATE_UNSPECIFIED = 0
    CODE_STATE_COMPLETED = 1
    CODE_STATE_FAILED = 2
    CODE_STATE_DESIGNER_WORKING = 3
    CODE_STATE_DESIGNER_COMPLETED = 4
    CODE_STATE_DESIGNER_FAILED = 5
    # ... 每个角色 3 个状态（WORKING/COMPLETED/FAILED）
    CODE_STATE_TESTER_WORKING = 21
    CODE_STATE_TESTER_COMPLETED = 22
    CODE_STATE_TESTER_FAILED = 23
```

**建议**：初期用 Todo 级别的布尔标志（`env_ready`, `code_generated`, `code_integrated`, `code_verified`），Phase 6 验收通过后再考虑升级为 CodeImplementState。

---

## 12. 工具权限矩阵

| 工具 | Supervisor | Designer | Planner | Builder | Generator | Integrator | Tester | Pentester | Reviewer | Reporter |
|------|:---------:|:--------:|:-------:|:-------:|:---------:|:----------:|:------:|:---------:|:--------:|:--------:|
| `terminal` | - | - | - | R/W | R/W | R/W | R | R/W | - | - |
| `file` | - | Workspace R | - | R/W | R/W | R/W | R | R/W | - | W |
| `browser` | - | R | - | - | - | - | - | R/W | - | - |
| `route_to_*` | W | - | - | - | - | - | - | - | - | - |
| `shared_state_patch` | W | W | W | W | W | W | W | W | W | W |
| `scope_contract` | R | W | R | - | - | - | - | - | R | R |
| `todo_list` | - | - | W | - | - | - | - | - | - | - |
| `todo_patch` | - | - | W | - | - | - | - | - | - | - |
| `integration_result` | - | - | - | - | - | W | - | - | - | - |
| `test_result` | - | - | - | - | - | - | W | - | - | - |
| `hack_result` | - | - | - | - | - | - | - | W | - | - |
| `auth_required` | - | - | - | - | - | - | - | W | - | - |
| `review_result` | - | - | - | - | - | - | - | - | W | - |
| `report_result` | - | - | - | - | - | - | - | - | - | W |
| `maintenance_result` | - | - | - | W | - | - | - | - | - | - |
| `code_result` | - | - | - | - | W | - | - | - | - | - |
| `artifact` | - | - | - | W | W | W | W | W | W | W |
| `search_*` | - | R | R | - | - | - | - | R | R | - |
| `*_in_memory` | - | R | R | R | R | - | - | R/W | R | R |
| `store_*` | - | - | - | W | W | - | - | W | - | - |
| `graphiti_search` | - | R | R | - | R | - | - | R | R | - |
| `ask` | W | W | W | W | W | W | W | W | W | W |

---

## 13. 实施顺序

### Phase 0：基础设施 + 规范模型（1-2 天）

1. 盘点并保留 `agentExecuteState / agentStateLogEntry / [agent-state]` 现有实现
2. 定义 Pydantic 规范模型（TaskStateEnum, AgentExecuteState, AgentStateList, SharedAgentState, SharedAgentStatePatch, Message, Artifact, StreamChunk, StructuredError）
3. 定义 Go mirror model（types.go 扩展）
4. 数据库迁移：新建 `todos`, `findings`, `evidence`, `artifacts`, `auth_requests` 表；`tasks` 表新增 context_id, state_id, protocol_version, shared_state, task_status_code 等字段
5. `tools/registry.go` 新增所有工具常量和结构体
6. Workspace 初始化服务
7. manifest 管理服务
8. 新增 structured error 类型
9. 更新 sqlc / GraphQL / frontend types

验收：
- Workspace 目录完整创建
- `status.json` 可写入和恢复
- manifest 可写入和查询
- 所有状态字段为枚举
- `[agent-state]` 事件仍可写入 msglogs，并可聚合为 `AgentStateList`

### Phase 1：Designer + Supervisor（2 天）

10. 新写 `prompts/designer.tmpl`
11. Go 端新增 `providers/designer.go` + API 端点
12. `providers/orchestrator.go` 新增 `DecideSupervisorStep()`
13. 新写 `prompts/supervisor.tmpl`
14. Python 端实现 `designer()` + `supervisor()` + 路由函数
15. 验证：用户输入 → Designer → scope_contract → Supervisor 路由

### Phase 2：Planner（2 天）

16. 改造 `controller/subtask.go` → `controller/todo.go`
17. 新写 `prompts/planner.tmpl`（合并 generator + refiner）
18. Python 端实现 `planner()`
19. 验证：scope_contract → Planner → Todo 列表

### Phase 3：执行节点（2 天）

20. `builder_node()` — 复用 installer handler
21. `generator_node()` — 复用 coder handler
22. 新写 `prompts/integrator.tmpl` + `providers/integrator.go`
23. 新写 `prompts/tester.tmpl` + `providers/tester.go`
24. 验证：Todo → Builder → Generator → Integrator → Tester

### Phase 4：Pentester 增强（2 天）

25. `pentester_node()` 多轮循环实现
26. `prompts/pentester.tmpl` 新增多轮循环 + auth_required
27. auth_requests 服务 + 授权恢复机制
28. 验证：Todo → Pentester 多轮 → findings

### Phase 5：Reviewer + Reporter（2 天）

29. 新写 `prompts/reviewer.tmpl` + `providers/reviewer.go`
30. `prompts/reporter.tmpl` 小改
31. `reporter_node()` 实现
32. 验证：所有 Todo 完成 → Reviewer → PASS → Reporter → END

### Phase 6：删除旧链路 + 全链路验收（1-2 天）

33. 重写 `orchestrator/app.py` 图拓扑（删除所有旧代码）
34. 删除旧 internal endpoint 调用
35. 删除新链路对 `primary_decision` / `current_subtask_id` 的依赖

验收测试场景：
- 信息缺失 → `INPUT_REQUIRED` → 补全后恢复
- 授权缺失 → `AUTH_REQUIRED` → 批准后恢复
- 策略拒绝 → `REJECTED` → END
- 代码任务 → Builder → Generator → Integrator → Tester → Reporter
- 渗透任务 → Pentester 执行 → 证据归档 → Reviewer 审查 → Reporter 报告
- 错误任务 → 结构化错误 → retryable 标识 → 最终 end 事件
- 中断恢复 → 从 `status.json` 和 `memory/checkpoints/` 恢复

必须运行：
- backend unit tests
- orchestrator graph tests
- Pydantic model tests
- DB migration tests
- permission allowlist tests
- Workspace path traversal tests
- stream event tests
- end-to-end task tests

### 总计：约 12 天
