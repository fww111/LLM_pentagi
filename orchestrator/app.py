from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any, Callable, Dict, Optional, TypedDict

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


class TaskState(TypedDict, total=False):
    flow_id: int
    task_id: int
    current_subtask_id: Optional[int]
    current_subtask_title: Optional[str]
    current_subtask_description: Optional[str]
    primary_msgchain_id: Optional[int]
    primary_decision: Optional[Dict[str, Any]]
    last_agent_result: Optional[str]
    task_status: Optional[str]
    task_result: Optional[str]
    failure_reason: Optional[str]


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


def generate_subtasks(state: TaskState) -> Dict[str, Any]:
    _go_post(f"tasks/{state['task_id']}/generate-subtasks", {"flow_id": state["flow_id"]})
    return {}


def select_next_subtask(state: TaskState) -> Dict[str, Any]:
    response = _go_post(f"tasks/{state['task_id']}/select-next-subtask", {"flow_id": state["flow_id"]})
    subtask = response.get("subtask")
    if not subtask:
        return {
            "current_subtask_id": None,
            "current_subtask_title": None,
            "current_subtask_description": None,
        }

    return {
        "current_subtask_id": subtask["id"],
        "current_subtask_title": subtask.get("title"),
        "current_subtask_description": subtask.get("description"),
    }


def route_after_select_next_subtask(state: TaskState) -> str:
    if state.get("current_subtask_id") is None:
        return "report_task_result"
    return "prepare_primary_agent_context"


def prepare_primary_agent_context(state: TaskState) -> Dict[str, Any]:
    response = _go_post(
        f"tasks/{state['task_id']}/subtasks/{state['current_subtask_id']}/prepare-primary-agent-context",
        {"flow_id": state["flow_id"]},
    )
    return {"primary_msgchain_id": response["msg_chain_id"]}


def primary_agent(state: TaskState) -> Dict[str, Any]:
    response = _go_post(
        f"tasks/{state['task_id']}/subtasks/{state['current_subtask_id']}/primary-agent-step",
        {"flow_id": state["flow_id"]},
    )
    return {"primary_decision": response["decision"]}


def route_after_primary_agent(state: TaskState) -> str:
    decision = state.get("primary_decision") or {}
    action = decision.get("action")

    if action == "call_agent":
        agent_type = decision.get("agent_type")
        if agent_type not in {"coder", "pentester", "searcher", "installer", "memorist", "adviser"}:
            raise RuntimeError(f"unsupported delegated agent type: {agent_type}")
        return agent_type
    if action == "input_required":
        return "input_required"
    if action == "completed":
        return "refine_subtasks"
    if action == "failed":
        return "failed"

    raise RuntimeError(f"unsupported primary_agent action: {action}")


def _execute_agent_node(agent_type: str) -> Callable[[TaskState], Dict[str, Any]]:
    def _node(state: TaskState) -> Dict[str, Any]:
        decision = state.get("primary_decision") or {}
        response = _go_post(
            f"tasks/{state['task_id']}/subtasks/{state['current_subtask_id']}/execute-agent",
            {
                "flow_id": state["flow_id"],
                "agent_type": agent_type,
                "payload": decision.get("payload") or {},
            },
        )
        execution = response["execution"]

        if execution.get("success"):
            writeback_result = execution.get("result", "")
        else:
            writeback_result = f"{agent_type} execution failed: {execution.get('error', 'unknown error')}"

        _go_post(
            f"tasks/{state['task_id']}/subtasks/{state['current_subtask_id']}/write-primary-agent-result",
            {
                "flow_id": state["flow_id"],
                "agent_type": agent_type,
                "tool_call_id": decision.get("tool_call_id", ""),
                "result": writeback_result,
            },
        )

        return {
            "last_agent_result": writeback_result,
            "primary_decision": None,
        }

    return _node


def input_required(state: TaskState) -> Dict[str, Any]:
    decision = state.get("primary_decision") or {}
    resume_value = interrupt(
        {
            "message": decision.get("message", ""),
            "flow_id": state["flow_id"],
            "task_id": state["task_id"],
            "subtask_id": state.get("current_subtask_id"),
        }
    )
    return {"primary_decision": None, "last_agent_result": str(resume_value)}


def refine_subtasks(state: TaskState) -> Dict[str, Any]:
    _go_post(f"tasks/{state['task_id']}/refine-subtasks", {"flow_id": state["flow_id"]})
    return {
        "primary_decision": None,
        "current_subtask_id": None,
        "current_subtask_title": None,
        "current_subtask_description": None,
    }


def report_task_result(state: TaskState) -> Dict[str, Any]:
    response = _go_post(f"tasks/{state['task_id']}/report-task-result", {"flow_id": state["flow_id"]})
    task = response["task"]
    return {
        "task_status": task.get("status"),
        "task_result": task.get("result"),
    }


def failed(state: TaskState) -> Dict[str, Any]:
    decision = state.get("primary_decision") or {}
    failure_reason = decision.get("error") or "primary_agent reported failure"
    response = _go_post(
        f"tasks/{state['task_id']}/fail-task",
        {"flow_id": state["flow_id"], "result": failure_reason},
    )
    task = response["task"]
    return {
        "task_status": task.get("status"),
        "task_result": task.get("result"),
        "failure_reason": failure_reason,
        "primary_decision": None,
    }


builder = StateGraph(TaskState)
builder.add_node("generate_subtasks", generate_subtasks)
builder.add_node("select_next_subtask", select_next_subtask)
builder.add_node("prepare_primary_agent_context", prepare_primary_agent_context)
builder.add_node("primary_agent", primary_agent)
builder.add_node("coder", _execute_agent_node("coder"))
builder.add_node("pentester", _execute_agent_node("pentester"))
builder.add_node("searcher", _execute_agent_node("searcher"))
builder.add_node("installer", _execute_agent_node("installer"))
builder.add_node("memorist", _execute_agent_node("memorist"))
builder.add_node("adviser", _execute_agent_node("adviser"))
builder.add_node("input_required", input_required)
builder.add_node("refine_subtasks", refine_subtasks)
builder.add_node("report_task_result", report_task_result)
builder.add_node("completed", lambda state: state)
builder.add_node("failed", failed)

builder.add_edge(START, "generate_subtasks")
builder.add_edge("generate_subtasks", "select_next_subtask")
builder.add_conditional_edges(
    "select_next_subtask",
    route_after_select_next_subtask,
    {
        "prepare_primary_agent_context": "prepare_primary_agent_context",
        "report_task_result": "report_task_result",
    },
)
builder.add_edge("prepare_primary_agent_context", "primary_agent")
builder.add_conditional_edges(
    "primary_agent",
    route_after_primary_agent,
    {
        "coder": "coder",
        "pentester": "pentester",
        "searcher": "searcher",
        "installer": "installer",
        "memorist": "memorist",
        "adviser": "adviser",
        "input_required": "input_required",
        "refine_subtasks": "refine_subtasks",
        "failed": "failed",
    },
)
builder.add_edge("coder", "primary_agent")
builder.add_edge("pentester", "primary_agent")
builder.add_edge("searcher", "primary_agent")
builder.add_edge("installer", "primary_agent")
builder.add_edge("memorist", "primary_agent")
builder.add_edge("adviser", "primary_agent")
builder.add_edge("input_required", "primary_agent")
builder.add_edge("refine_subtasks", "select_next_subtask")
builder.add_edge("report_task_result", "completed")
builder.add_edge("completed", END)
builder.add_edge("failed", END)

GRAPH = builder.compile(checkpointer=CHECKPOINTER)

app = FastAPI(title="PentAGI LangGraph Orchestrator", version="0.1.0")


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
    return {"status": "ok"}


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
