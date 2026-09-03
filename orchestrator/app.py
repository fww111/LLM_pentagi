from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any, Callable, Dict, List, Optional, TypedDict

import requests
from fastapi import FastAPI, HTTPException
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt
from pydantic import BaseModel

try:
    from langgraph.checkpoint.sqlite import SqliteSaver
except Exception:  # pragma: no cover - fallback for missing optional dependency
    SqliteSaver = None


logging.basicConfig(level=os.getenv("ORCHESTRATOR_LOG_LEVEL", "INFO"))
LOGGER = logging.getLogger("pentagentx-langgraph")

GO_INTERNAL_BASE_URL = os.getenv(
    "PENTAGI_INTERNAL_BASE_URL",
    "http://127.0.0.1:8080/api/v1/internal/orchestrator",
).rstrip("/")
INTERNAL_TOKEN = os.getenv("LANGGRAPH_INTERNAL_TOKEN", "")
VERIFY_INTERNAL_SSL = os.getenv("PENTAGI_INTERNAL_VERIFY_SSL", "true").lower() not in {"0", "false", "no"}
CHECKPOINT_PATH = os.getenv(
    "LANGGRAPH_CHECKPOINT_PATH",
    os.path.join(os.path.dirname(__file__), "langgraph-checkpoints.sqlite"),
)

SESSION = requests.Session()
SESSION.verify = VERIFY_INTERNAL_SSL
if INTERNAL_TOKEN:
    SESSION.headers["X-Pentagi-Internal-Token"] = INTERNAL_TOKEN


class RunTaskRequest(BaseModel):
    flow_id: int
    task_id: int

class ResumeTaskRequest(BaseModel):
    flow_id: int
    task_id: int
    user_input: str = ""

def _checkpointer() -> Any:
    if SqliteSaver is None:
        LOGGER.warning("SqliteSaver unavailable, falling back to in-memory checkpointing")
        return InMemorySaver()

    # Patch JsonPlusSerializer: langgraph-checkpoint-sqlite 2.0.x calls .dumps()
    # but langgraph-checkpoint 4.x only provides dumps_typed/loads_typed.
    # Add a compatibility shim so the checkpointer doesn't crash.
    try:
        from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer as _JPS
        if not hasattr(_JPS, "dumps"):
            import json

            def _dumps_shim(self, obj: Any) -> bytes:
                """Compatibility shim: serialize to JSON bytes."""
                return json.dumps(obj, default=str).encode("utf-8")

            def _loads_shim(self, data: bytes) -> Any:
                """Compatibility shim: deserialize from JSON bytes."""
                return json.loads(data)

            _JPS.dumps = _dumps_shim
            _JPS.loads = _loads_shim
            LOGGER.info("Patched JsonPlusSerializer.dumps/loads for compatibility")
    except Exception as exc:
        LOGGER.warning("Failed to patch JsonPlusSerializer: %s", exc)

    os.makedirs(os.path.dirname(CHECKPOINT_PATH), exist_ok=True)
    conn = sqlite3.connect(CHECKPOINT_PATH, check_same_thread=False)
    return SqliteSaver(conn)


CHECKPOINTER = _checkpointer()


def _thread_config(task_id: int) -> Dict[str, Any]:
    return {"configurable": {"thread_id": str(task_id)}}


def _go_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{GO_INTERNAL_BASE_URL}/{path.lstrip('/')}"
    try:
        response = SESSION.post(url, json=payload, timeout=600)
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=504,
            detail={
                "message": f"go internal orchestrator call failed: {exc}",
                "url": url,
            },
        ) from exc
    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "message": f"go internal orchestrator call failed: {response.status_code}",
                "url": url,
                "body": response.text,
            },
        )
    return response.json()


def _exception_message(exc: Exception) -> str:
    detail = getattr(exc, "detail", None)
    if isinstance(detail, dict):
        message = detail.get("message")
        if message:
            return str(message)
    if detail:
        return str(detail)
    return str(exc)


# ========================================
# Multi-agent state and graph
# ========================================

AGENT_ROLES = [
    "builder", "generator", "integrator", "tester",
    "pentester", "reviewer", "reporter", "researcher",
]


class MultiAgentState(TypedDict, total=False):
    flow_id: int
    task_id: int
    context_id: Optional[str]
    scope_contract: Optional[Dict[str, Any]]
    todo_plan: Optional[List[Dict[str, Any]]]
    plan_needs_update: Optional[bool]
    active_todo_id: Optional[str]
    active_todo: Optional[Dict[str, Any]]
    supervisor_decision: Optional[Dict[str, Any]]
    last_agent_result: Optional[str]
    shared_state: Optional[Dict[str, Any]]
    task_status: Optional[str]
    task_result: Optional[str]
    failure_reason: Optional[str]
    auth_request: Optional[Dict[str, Any]]
    designer_msg_chain_id: Optional[int]
    planner_msg_chain_id: Optional[int]
    supervisor_msg_chain_id: Optional[int]


# --- Multi-agent nodes ---

def designer(state: MultiAgentState) -> Dict[str, Any]:
    response = _go_post(
        f"tasks/{state['task_id']}/designer-step",
        {"flow_id": state["flow_id"], "msg_chain_id": state.get("designer_msg_chain_id") or 0},
    )
    decision = response.get("decision", {})
    scope_contract = decision.get("result", {})
    return {
        "scope_contract": scope_contract,
        "supervisor_decision": decision,
        "designer_msg_chain_id": decision.get("msg_chain_id", 0),
    }


def route_after_designer(state: MultiAgentState) -> str:
    decision = state.get("supervisor_decision") or {}
    action = decision.get("action", "")
    if action == "input_required":
        return "input_required"
    # scope_contract completed → planner
    return "planner"


def planner(state: MultiAgentState) -> Dict[str, Any]:
    """Planner node: calls planner-step to let the LLM decide todo plan via TodoList/TodoPatch tools."""
    response = _go_post(
        f"tasks/{state['task_id']}/planner-step",
        {
            "flow_id": state["flow_id"],
            "msg_chain_id": state.get("planner_msg_chain_id") or 0,
            "has_existing_plan": bool(state.get("todo_plan")),
        },
    )
    decision = response.get("decision", {})
    todos = decision.get("result", {})
    action = decision.get("action", "")
    # Accept both plan_ready (correct semantics) and completed (backward compat)
    if action not in ("plan_ready", "completed"):
        LOGGER.warning("planner returned unexpected action %s, treating as plan_ready", action)
    if isinstance(todos, str):
        import json as _json
        try:
            todos = _json.loads(todos)
        except Exception:
            todos = None
    if isinstance(todos, dict) and "todos" in todos:
        todos = todos["todos"]
    todos = _normalize_todo_plan(todos or state.get("todo_plan", []))

    return {
        "todo_plan": todos,
        "active_todo_id": None,
        "active_todo": None,
        "plan_needs_update": False,
        "supervisor_decision": decision,
        "planner_msg_chain_id": decision.get("msg_chain_id", 0),
    }


def supervisor(state: MultiAgentState) -> Dict[str, Any]:
    response = _go_post(
        f"tasks/{state['task_id']}/supervisor-step",
        {"flow_id": state["flow_id"], "msg_chain_id": state.get("supervisor_msg_chain_id") or 0},
    )
    decision = response.get("decision", {})
    if (
        decision.get("action") == "completed"
        and _has_open_todos(state)
        and not _is_final_reporter_fallback(decision)
    ):
        next_todo = _select_next_todo(state)
        if next_todo:
            decision = {
                "action": "delegate",
                "agent_role": _todo_owner_agent(next_todo),
                "todo_id": next_todo.get("todo_id") or next_todo.get("id"),
                "todo": next_todo,
                "msg_chain_id": decision.get("msg_chain_id", 0),
                "message": "continue pending todo before completing task",
            }
        else:
            decision = {
                "action": "failed",
                "msg_chain_id": decision.get("msg_chain_id", 0),
                "message": "cannot complete task because open todos remain but none are runnable",
                "error": "open todos remain with unsatisfied dependencies",
            }
    elif decision.get("action") not in {
        "delegate",
        "auth_required",
        "input_required",
        "completed",
        "failed",
        "rejected",
    }:
        next_todo = _select_next_todo(state)
        if next_todo:
            decision = {
                "action": "delegate",
                "agent_role": _todo_owner_agent(next_todo),
                "todo_id": _todo_id(next_todo),
                "todo": next_todo,
                "msg_chain_id": decision.get("msg_chain_id", 0),
                "message": "fallback delegated after supervisor returned no structured route",
            }
        elif _has_open_todos(state):
            decision = {
                "action": "failed",
                "msg_chain_id": decision.get("msg_chain_id", 0),
                "message": "fallback failed because open todos remain but none are runnable",
                "error": "open todos remain with unsatisfied dependencies",
            }
        else:
            decision = {
                "action": "completed",
                "msg_chain_id": decision.get("msg_chain_id", 0),
                "message": "fallback completed because no open todos remain",
            }
    active_todo = decision.get("todo") or state.get("active_todo")
    active_todo_id = (
        decision.get("todo_id")
        or decision.get("active_todo_id")
        or (active_todo or {}).get("todo_id")
        or (active_todo or {}).get("id")
        or state.get("active_todo_id")
    )
    if decision.get("action") == "delegate" and not active_todo_id:
        active_todo = _select_next_todo(state, decision.get("agent_role"))
        active_todo_id = (active_todo or {}).get("todo_id") or (active_todo or {}).get("id")
    return {
        "supervisor_decision": decision,
        "active_todo": active_todo,
        "active_todo_id": str(active_todo_id) if active_todo_id is not None else None,
        # If supervisor delegates back to planner, mark plan as needing update
        "plan_needs_update": decision.get("action") == "delegate" and decision.get("agent_role") == "planner",
        "supervisor_msg_chain_id": decision.get("msg_chain_id", 0),
    }


def route_after_supervisor(state: MultiAgentState) -> str:
    decision = state.get("supervisor_decision") or {}
    action = decision.get("action", "")

    if action == "delegate":
        agent_role = decision.get("agent_role", "")
        if agent_role == "planner":
            return "planner"
        if agent_role in AGENT_ROLES:
            return agent_role
        LOGGER.warning(f"Unknown agent role from supervisor: {agent_role}, defaulting to reporter")
        return "reporter"
    if action == "auth_required":
        return "auth_required"
    if action == "input_required":
        return "input_required"
    if action == "completed":
        return "completed"
    if action == "failed":
        return "failed"
    if action == "rejected":
        return "rejected"

    LOGGER.warning(f"Unknown supervisor action after fallback: {action}, defaulting to failed")
    return "failed"


def _agent_question(agent_role: str, state: MultiAgentState) -> str:
    active_todo = state.get("active_todo") or {}
    shared_state = state.get("shared_state") or {}
    scope_contract = state.get("scope_contract") or {}
    last_result = state.get("last_agent_result") or ""

    title = active_todo.get("title") or f"{agent_role} execution"
    success_criteria = active_todo.get("success_criteria") or "Complete the delegated work and return concrete evidence."
    inputs = active_todo.get("inputs") or ""

    execution_rules = [
        "Execution rules:",
        "- Use only non-interactive one-shot commands; every command must finish by itself.",
        "- Do not open interactive shells for service checks; prefer bounded commands with explicit timeouts.",
        "- Do not install packages or troubleshoot Docker unless explicitly requested.",
        "- Use tools that are already available in the execution environment.",
        "- Record concrete command output, observations, and errors as evidence for this todo.",
    ]

    return "\n".join([
        f"Agent role: {agent_role}",
        f"Active todo id: {state.get('active_todo_id') or 'unknown'}",
        f"Todo title: {title}",
        f"Inputs: {inputs}",
        f"Success criteria: {success_criteria}",
        f"Scope contract: {scope_contract}",
        f"Shared state: {shared_state}",
        f"Previous agent result: {last_result}",
        *execution_rules,
        "Execute this todo within scope, update evidence, and return a structured result.",
    ])


def _agent_payload(agent_role: str, state: MultiAgentState) -> Dict[str, Any]:
    decision = state.get("supervisor_decision") or {}
    payload = dict(decision.get("payload") or {})
    if not payload.get("question"):
        payload["question"] = _agent_question(agent_role, state)
    if not payload.get("message"):
        payload["message"] = f"Run {agent_role} for todo {state.get('active_todo_id') or 'current task'}"
    return payload


def _todo_owner_agent(todo: Dict[str, Any]) -> str:
    return str(todo.get("owner_agent") or todo.get("owner") or "pentester")


def _todo_id(todo: Dict[str, Any]) -> str:
    return str(todo.get("todo_id") or todo.get("id") or "")


def _todo_status(todo: Dict[str, Any]) -> str:
    return str(todo.get("status") or "pending").strip().lower()


def _is_open_todo(todo: Dict[str, Any]) -> bool:
    return _todo_status(todo) in (
        "",
        "pending",
        "created",
        "queued",
        "not_started",
        "running",
        "in_progress",
        "blocked",
    )


def _is_completed_todo(todo: Dict[str, Any]) -> bool:
    return _todo_status(todo) in ("completed", "finished", "done", "success", "skipped")


def _is_reporter_todo(todo: Dict[str, Any]) -> bool:
    return _todo_owner_agent(todo) == "reporter"


def _is_final_reporter_fallback(decision: Dict[str, Any]) -> bool:
    message = str(decision.get("message") or "").strip().lower()
    return "final reporter" in message or "reporter text" in message


def _todo_dependencies(todo: Dict[str, Any]) -> List[str]:
    deps = todo.get("depends_on") or todo.get("dependencies") or []
    if isinstance(deps, str):
        deps = [item.strip() for item in deps.split(",")]
    if not isinstance(deps, list):
        return []
    return [str(item).strip() for item in deps if str(item).strip()]


def _dependencies_satisfied(todo: Dict[str, Any], todos_by_id: Dict[str, Dict[str, Any]]) -> bool:
    for dep_id in _todo_dependencies(todo):
        dep = todos_by_id.get(dep_id)
        if dep is None or not _is_completed_todo(dep):
            return False
    return True


def _non_reporter_todos_closed(todos: List[Dict[str, Any]]) -> bool:
    return all((not _is_open_todo(todo)) for todo in todos if not _is_reporter_todo(todo))


def _has_open_todos(state: MultiAgentState) -> bool:
    return any(_is_open_todo(todo) for todo in _todo_plan_items(state.get("todo_plan")))


def _select_next_todo(state: MultiAgentState, agent_role: Optional[str] = None) -> Optional[Dict[str, Any]]:
    todos = _todo_plan_items(state.get("todo_plan"))
    todos_by_id = {_todo_id(todo): todo for todo in todos if _todo_id(todo)}

    candidates = [
        todo for todo in todos
        if _is_open_todo(todo)
        and (not agent_role or _todo_owner_agent(todo) == agent_role)
        and _dependencies_satisfied(todo, todos_by_id)
    ]
    if not candidates:
        return None

    for todo in candidates:
        if not _is_reporter_todo(todo):
            return todo

    if _non_reporter_todos_closed(todos):
        return candidates[0]
    return None


def _update_todo_status(
    todos: Optional[List[Dict[str, Any]]],
    todo_id: str,
    status: str,
    result: str,
) -> Optional[List[Dict[str, Any]]]:
    items = _todo_plan_items(todos)
    if not items or not todo_id:
        return items
    updated = []
    for todo in items:
        item = dict(todo)
        if str(item.get("todo_id") or item.get("id")) == str(todo_id):
            item["status"] = status
            item["result"] = result
        updated.append(item)
    return updated


def _todo_plan_items(plan: Any) -> List[Dict[str, Any]]:
    if isinstance(plan, dict):
        plan = plan.get("todos") or plan.get("items") or []
    if not isinstance(plan, list):
        return []
    return [item for item in plan if isinstance(item, dict)]


def _normalize_todo_plan(plan: Any) -> List[Dict[str, Any]]:
    return [dict(item) for item in _todo_plan_items(plan)]


def _multi_agent_execute(agent_role: str) -> Callable[[MultiAgentState], Dict[str, Any]]:
    def _node(state: MultiAgentState) -> Dict[str, Any]:
        todo_id = state.get("active_todo_id") or ""

        try:
            response = _go_post(
                f"tasks/{state['task_id']}/agent-execute",
                {
                    "flow_id": state["flow_id"],
                    "agent_role": agent_role,
                    "todo_id": todo_id,
                    "payload": _agent_payload(agent_role, state),
                },
            )
        except Exception as exc:
            execution_result = f"{agent_role} execution failed: {_exception_message(exc)}"
            LOGGER.warning(
                "agent execution failed without crashing graph",
                extra={"task_id": state.get("task_id"), "agent_role": agent_role, "todo_id": todo_id},
            )
            return {
                "last_agent_result": execution_result,
                "todo_plan": _update_todo_status(state.get("todo_plan"), todo_id, "failed", execution_result),
                "active_todo_id": None,
                "active_todo": None,
                "supervisor_decision": None,
            }
        execution = response.get("result", response.get("execution", {}))
        execution_result = execution.get("result", "")
        execution_status = "completed" if execution.get("success") else "failed"

        # Update shared state on success
        if execution.get("success"):
            _go_post(
                f"tasks/{state['task_id']}/update-shared-state",
                {
                    "flow_id": state["flow_id"],
                    "active_node": agent_role,
                    "active_todo_id": todo_id,
                    "updates": {"last_result": execution.get("result", "")},
                },
            )

        return {
            "last_agent_result": execution_result,
            "todo_plan": _update_todo_status(state.get("todo_plan"), todo_id, execution_status, execution_result),
            "active_todo_id": None,
            "active_todo": None,
            "supervisor_decision": None,
        }

    return _node


def auth_required(state: MultiAgentState) -> Dict[str, Any]:
    decision = state.get("supervisor_decision") or {}
    todo_id = state.get("active_todo_id", "")

    _go_post(
        f"tasks/{state['task_id']}/store-auth-request",
        {
            "flow_id": state["flow_id"],
            "todo_id": todo_id,
            "action": decision.get("result", ""),
            "risk_level": "high",
            "justification": decision.get("message", "High-risk operation requires authorization"),
        },
    )

    # Interrupt and wait for human approval
    resume_value = interrupt(
        {
            "message": decision.get("message", "Authorization required"),
            "flow_id": state["flow_id"],
            "task_id": state["task_id"],
            "todo_id": todo_id,
            "action": decision.get("result", ""),
        }
    )

    approval = str(resume_value)
    if approval.lower() in ("approved", "yes", "true"):
        return {"supervisor_decision": None, "last_agent_result": "Authorization approved"}
    else:
        return {"supervisor_decision": None, "last_agent_result": "Authorization rejected"}


def ma_input_required(state: MultiAgentState) -> Dict[str, Any]:
    decision = state.get("supervisor_decision") or {}
    resume_value = interrupt(
        {
            "message": decision.get("message", ""),
            "flow_id": state["flow_id"],
            "task_id": state["task_id"],
        }
    )
    return {"supervisor_decision": None, "last_agent_result": str(resume_value)}


def ma_completed(state: MultiAgentState) -> Dict[str, Any]:
    try:
        response = _go_post(
            f"tasks/{state['task_id']}/complete-task",
            {"flow_id": state["flow_id"]},
        )
    except HTTPException as exc:
        failure_reason = f"complete-task failed: {exc.detail}"
        _go_post(
            f"tasks/{state['task_id']}/fail-task",
            {"flow_id": state["flow_id"], "result": failure_reason},
        )
        return {
            "task_status": "failed",
            "task_result": failure_reason,
            "failure_reason": failure_reason,
            "supervisor_decision": None,
        }
    return {
        "task_status": "completed",
        "task_result": response.get("task", {}).get("result", ""),
        "supervisor_decision": None,
    }


def ma_rejected(state: MultiAgentState) -> Dict[str, Any]:
    decision = state.get("supervisor_decision") or {}
    reason = decision.get("error") or decision.get("message") or "Task rejected"
    _go_post(
        f"tasks/{state['task_id']}/reject-task",
        {"flow_id": state["flow_id"], "result": reason},
    )
    return {
        "task_status": "rejected",
        "task_result": reason,
        "failure_reason": reason,
        "supervisor_decision": None,
    }


def ma_failed(state: MultiAgentState) -> Dict[str, Any]:
    decision = state.get("supervisor_decision") or {}
    failure_reason = decision.get("error") or "Task failed"
    _go_post(
        f"tasks/{state['task_id']}/fail-task",
        {"flow_id": state["flow_id"], "result": failure_reason},
    )
    return {
        "task_status": "failed",
        "task_result": failure_reason,
        "failure_reason": failure_reason,
        "supervisor_decision": None,
    }


def _build_graph() -> StateGraph:
    builder = StateGraph(MultiAgentState)

    # Main pipeline nodes
    builder.add_node("designer", designer)
    builder.add_node("planner", planner)
    builder.add_node("supervisor", supervisor)

    # Agent execution nodes (registered uniformly from the factory)
    for role in AGENT_ROLES:
        builder.add_node(role, _multi_agent_execute(role))

    # Terminal / interrupt nodes
    builder.add_node("auth_required", auth_required)
    builder.add_node("input_required", ma_input_required)
    builder.add_node("completed", ma_completed)
    builder.add_node("rejected", ma_rejected)
    builder.add_node("failed", ma_failed)

    # Entry → designer
    builder.add_edge(START, "designer")

    # Designer → route (planner or input_required)
    builder.add_conditional_edges(
        "designer",
        route_after_designer,
        {
            "planner": "planner",
            "input_required": "input_required",
        },
    )
    builder.add_edge("input_required", "designer")

    # Planner → supervisor
    builder.add_edge("planner", "supervisor")

    # Supervisor → route to agent or terminal
    builder.add_conditional_edges(
        "supervisor",
        route_after_supervisor,
        {role: role for role in AGENT_ROLES} | {
            "planner": "planner",
            "auth_required": "auth_required",
            "input_required": "input_required",
            "completed": "completed",
            "rejected": "rejected",
            "failed": "failed",
        },
    )

    # All agent nodes loop back to supervisor
    for role in AGENT_ROLES:
        builder.add_edge(role, "supervisor")

    builder.add_edge("auth_required", "supervisor")
    builder.add_edge("completed", END)
    builder.add_edge("rejected", END)
    builder.add_edge("failed", END)

    return builder


GRAPH = _build_graph().compile(checkpointer=CHECKPOINTER)
LOGGER.info("PentAgentX LangGraph orchestrator initialized (multi-agent topology)")


# ========================================
# FastAPI app
# ========================================

app = FastAPI(title="PentAgentX LangGraph Orchestrator", version="0.3.0")


def _serialize_snapshot(task_id: int) -> Dict[str, Any]:
    snapshot = GRAPH.get_state(_thread_config(task_id))
    values = getattr(snapshot, "values", {}) or {}
    next_nodes = list(getattr(snapshot, "next", ()) or ())
    interrupts = []

    for item in getattr(snapshot, "interrupts", ()) or ():
        interrupts.append(
            {
                "id": getattr(item, "id", None),
                "value": getattr(item, "value", None),
            }
        )

    if interrupts:
        status = "waiting"
    elif next_nodes:
        status = "running"
    else:
        status = "completed"

    return {
        "status": status,
        "task": {
            "flow_id": values.get("flow_id"),
            "task_id": values.get("task_id"),
            "task_status": values.get("task_status"),
            "task_result": values.get("task_result"),
        },
        "next": next_nodes,
        "interrupts": interrupts,
    }


def _run_stream(input_value: Any, task_id: int) -> Dict[str, Any]:
    config = _thread_config(task_id)
    try:
        for _ in GRAPH.stream(input_value, config, stream_mode="values"):
            pass
    except Exception as exc:
        reason = _exception_message(exc)
        LOGGER.exception("orchestrator graph failed; marking task failed")
        flow_id = None
        if isinstance(input_value, dict):
            flow_id = input_value.get("flow_id")
        if flow_id is None:
            values = getattr(GRAPH.get_state(config), "values", {}) or {}
            flow_id = values.get("flow_id")
        if flow_id:
            try:
                _go_post(f"tasks/{task_id}/fail-task", {"flow_id": flow_id, "result": reason})
            except Exception:
                LOGGER.exception("failed to mark task failed after graph exception")
    return _serialize_snapshot(task_id)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "graph_mode": "multi_agent"}


@app.post("/runs/start")
def start_run(req: RunTaskRequest) -> Dict[str, Any]:
    return _run_stream({"flow_id": req.flow_id, "task_id": req.task_id}, req.task_id)


@app.post("/runs/resume")
def resume_run(req: ResumeTaskRequest) -> Dict[str, Any]:
    snapshot = GRAPH.get_state(_thread_config(req.task_id))
    interrupts = list(getattr(snapshot, "interrupts", ()) or ())
    next_nodes = list(getattr(snapshot, "next", ()) or ())

    if interrupts:
        resume_val = req.user_input if req.user_input else {"resumed": True}
        return _run_stream(Command(resume=resume_val), req.task_id)

    if next_nodes:
        return _run_stream(None, req.task_id)

    values = getattr(snapshot, "values", {}) or {}
    if values:
        return _serialize_snapshot(req.task_id)

    return _run_stream({"flow_id": req.flow_id, "task_id": req.task_id}, req.task_id)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host=os.getenv("LANGGRAPH_HOST", "0.0.0.0"),
        port=int(os.getenv("LANGGRAPH_PORT", "8091")),
        reload=False,
    )
