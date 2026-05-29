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
LOGGER = logging.getLogger("pentagi-langgraph")

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


def _checkpointer() -> Any:
    if SqliteSaver is None:
        LOGGER.warning("SqliteSaver unavailable, falling back to in-memory checkpointing")
        return InMemorySaver()

    os.makedirs(os.path.dirname(CHECKPOINT_PATH), exist_ok=True)
    conn = sqlite3.connect(CHECKPOINT_PATH, check_same_thread=False)
    return SqliteSaver(conn)


CHECKPOINTER = _checkpointer()


def _thread_config(task_id: int) -> Dict[str, Any]:
    return {"configurable": {"thread_id": str(task_id)}}


def _go_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{GO_INTERNAL_BASE_URL}/{path.lstrip('/')}"
    response = SESSION.post(url, json=payload, timeout=600)
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
    active_todo_id: Optional[str]
    active_todo: Optional[Dict[str, Any]]
    supervisor_decision: Optional[Dict[str, Any]]
    last_agent_result: Optional[str]
    shared_state: Optional[Dict[str, Any]]
    task_status: Optional[str]
    task_result: Optional[str]
    failure_reason: Optional[str]
    auth_request: Optional[Dict[str, Any]]


# --- Multi-agent nodes ---

def designer(state: MultiAgentState) -> Dict[str, Any]:
    response = _go_post(
        f"tasks/{state['task_id']}/designer-step",
        {"flow_id": state["flow_id"]},
    )
    decision = response.get("decision", {})
    scope_contract = decision.get("result", {})
    return {"scope_contract": scope_contract, "supervisor_decision": decision}


def route_after_designer(state: MultiAgentState) -> str:
    decision = state.get("supervisor_decision") or {}
    action = decision.get("action", "")
    if action == "input_required":
        return "input_required"
    # scope_contract completed → planner
    return "planner"


def planner(state: MultiAgentState) -> Dict[str, Any]:
    response = _go_post(
        f"tasks/{state['task_id']}/planner-step",
        {"flow_id": state["flow_id"]},
    )
    decision = response.get("decision", {})
    return {"supervisor_decision": decision}


def supervisor(state: MultiAgentState) -> Dict[str, Any]:
    response = _go_post(
        f"tasks/{state['task_id']}/supervisor-step",
        {"flow_id": state["flow_id"]},
    )
    decision = response.get("decision", {})
    return {"supervisor_decision": decision}


def route_after_supervisor(state: MultiAgentState) -> str:
    decision = state.get("supervisor_decision") or {}
    action = decision.get("action", "")

    if action == "delegate":
        agent_role = decision.get("agent_role", "")
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

    LOGGER.warning(f"Unknown supervisor action: {action}, defaulting to failed")
    return "failed"


def _multi_agent_execute(agent_role: str) -> Callable[[MultiAgentState], Dict[str, Any]]:
    def _node(state: MultiAgentState) -> Dict[str, Any]:
        decision = state.get("supervisor_decision") or {}
        todo_id = state.get("active_todo_id", "")

        # Map new agent roles to legacy Go handler names
        go_agent_type = agent_role
        if agent_role == "builder":
            go_agent_type = "installer"
        elif agent_role == "generator":
            go_agent_type = "coder"

        response = _go_post(
            f"tasks/{state['task_id']}/execute-agent",
            {
                "flow_id": state["flow_id"],
                "agent_type": go_agent_type,
                "payload": decision.get("payload") or {},
            },
        )
        execution = response.get("execution", {})

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
            "last_agent_result": execution.get("result", ""),
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
    response = _go_post(
        f"tasks/{state['task_id']}/complete-task",
        {"flow_id": state["flow_id"]},
    )
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

    # Agent execution nodes
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
LOGGER.info("PentAGI LangGraph orchestrator initialized (multi-agent topology)")


# ========================================
# FastAPI app
# ========================================

app = FastAPI(title="PentAGI LangGraph Orchestrator", version="0.3.0")


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
    for _ in GRAPH.stream(input_value, config, stream_mode="values"):
        pass
    return _serialize_snapshot(task_id)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "graph_mode": "multi_agent"}


@app.post("/runs/start")
def start_run(req: RunTaskRequest) -> Dict[str, Any]:
    return _run_stream({"flow_id": req.flow_id, "task_id": req.task_id}, req.task_id)


@app.post("/runs/resume")
def resume_run(req: RunTaskRequest) -> Dict[str, Any]:
    snapshot = GRAPH.get_state(_thread_config(req.task_id))
    interrupts = list(getattr(snapshot, "interrupts", ()) or ())
    next_nodes = list(getattr(snapshot, "next", ()) or ())

    if interrupts:
        return _run_stream(Command(resume={"resumed": True}), req.task_id)

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
