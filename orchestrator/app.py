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
# Feature flag: set to "multi_agent" to use the new graph, "legacy" for old
GRAPH_MODE = os.getenv("PENTAGI_GRAPH_MODE", "legacy")

SESSION = requests.Session()
SESSION.verify = VERIFY_INTERNAL_SSL
if INTERNAL_TOKEN:
    SESSION.headers["X-Pentagi-Internal-Token"] = INTERNAL_TOKEN


class RunTaskRequest(BaseModel):
    flow_id: int
    task_id: int


# ========================================
# Legacy TaskState and graph (kept for backward compatibility)
# ========================================

class TaskState(TypedDict, total=False):
      """Shared state for task execution in LangGraph"""
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
      workspace: Optional[Dict[str, Any]]
      step_count: Optional[int]
      total_steps: Optional[int]
      is_completed: Optional[bool]
      has_error: Optional[bool]
      review_result: Optional[Dict[str, Any]]


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
# Legacy graph nodes (unchanged)
# ========================================

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

 def reviewer_node(state: TaskState) -> Dict[str, Any]:
      """Execute reviewer agent to validate penetration test results"""
      response = _go_post(
          f"tasks/{state['task_id']}/execute-agent",
          {
              "flow_id": state["flow_id"],
              "agent_type": "reviewer",
              "payload": {
                  "scope_contract": state.get("workspace", {}).get("scope_contract", ""),
                  "plan": state.get("workspace", {}).get("plan", ""),
                  "findings": state.get("workspace", {}).get("findings", ""),
                  "evidence": state.get("workspace", {}).get("evidence", ""),
                  "message": "Please review the penetration test results for quality and
  compliance",
              },
          },
      )
      execution = response.get("execution", {})

      if execution.get("success"):
          result = execution.get("result", "")
          # Parse the JSON result to extract verdict
          try:
              import json
              review_data = json.loads(result)
              verdict = review_data.get("verdict", "FAIL")
              comments = review_data.get("comments", "")
              suggestions = review_data.get("suggestions", "")

              updated_state = {
                  "last_agent_result": f"Reviewer completed with verdict: {verdict}",
                  "review_result": {
                      "verdict": verdict,
                      "comments": comments,
                      "suggestions": suggestions,
                  },
                  "workspace": state.get("workspace", {}),
              }
              updated_state["workspace"]["review_result"] = {
                  "verdict": verdict,
                  "comments": comments,
                  "suggestions": suggestions,
              }

              return updated_state
          except json.JSONDecodeError:
              return {
                  "last_agent_result": "Reviewer completed but failed to parse result",
                  "review_result": {"verdict": "FAIL", "comments": "Result parsing failed",
  "suggestions": ""},
                  "workspace": state.get("workspace", {}),
              }
      else:
          result = execution.get("error", "unknown error")
          return {
              "last_agent_result": f"Reviewer execution failed: {result}",
              "review_result": {"verdict": "FAIL", "comments": result, "suggestions": ""},
              "workspace": state.get("workspace", {}),
          }


  def reporter_node(state: TaskState) -> Dict[str, Any]:
      """Execute reporter agent to generate final report after successful review"""
      # Only execute if review passed
      review_result = state.get("review_result", {})
      if review_result.get("verdict") != "PASS":
          return {
              "last_agent_result": "Skipping reporter - review did not pass",
              "task_status": "failed",
              "failure_reason": "Review failed - cannot generate report",
          }

      response = _go_post(
          f"tasks/{state['task_id']}/execute-agent",
          {
              "flow_id": state["flow_id"],
              "agent_type": "reporter",
              "payload": {
                  "input": state.get("workspace", {}).get("task_context", ""),
                  "message": "Generate final penetration test report",
              },
          },
      )
      execution = response.get("execution", {})

      if execution.get("success"):
          result = execution.get("result", "")
          return {
              "last_agent_result": "Reporter completed successfully",
              "task_result": result,
              "task_status": "completed",
              "normalized_state": "COMPLETED",
              "workspace": state.get("workspace", {}),
          }
      else:
          result = execution.get("error", "unknown error")
          return {
              "last_agent_result": f"Reporter execution failed: {result}",
              "task_status": "failed",
              "failure_reason": result,
          }


  def supervisor_node(state: TaskState) -> Dict[str, Any]:
      """Supervisor node to handle failed review and determine next steps"""
      review_result = state.get("review_result", {})
      failure_reason = review_result.get("comments", "Review failed")

      return {
          "last_agent_result": f"Review failed: {failure_reason}",
          "task_status": "failed",
          "failure_reason": f"Security review failed: {failure_reason}",
          "workspace": state.get("workspace", {}),
      }


  def route_after_reviewer(state: TaskState) -> str:
      """Route after reviewer node based on verdict"""
      review_result = state.get("review_result", {})
      verdict = review_result.get("verdict", "FAIL")

      if verdict == "PASS":
          return "reporter"
      else:
          return "supervisor"


  def route_after_reporter(state: TaskState) -> str:
      """Route after reporter node"""
      return "END"

def _build_legacy_graph() -> StateGraph:
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
    builder.add_node("reviewer", reviewer_node)
    builder.add_node("reporter", reporter_node)
    builder.add_node("supervisor", supervisor_node)
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
          "reviewer": "reviewer",  # Add reviewer route
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

  # Add reviewer conditional routing
  builder.add_conditional_edges(
      "reviewer",
      route_after_reviewer,
      {
          "reporter": "reporter",
          "supervisor": "supervisor",
      },
  )
  # Add reporter to END, supervisor to failed
  builder.add_edge("reporter", "END")
  builder.add_edge("supervisor", "failed")
  builder.add_edge("report_task_result", "completed")
  builder.add_edge("completed", END)
  builder.add_edge("failed", END)
    return builder


# ========================================
# Multi-agent TaskState and graph
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

        response = _go_post(
            f"tasks/{state['task_id']}/agent-execute",
            {
                "flow_id": state["flow_id"],
                "agent_role": agent_role,
                "todo_id": todo_id,
                "payload": decision.get("payload") or {},
            },
        )
        result = response.get("result", {})

        # Update shared state
        if result.get("success"):
            _go_post(
                f"tasks/{state['task_id']}/update-shared-state",
                {
                    "flow_id": state["flow_id"],
                    "active_node": agent_role,
                    "active_todo_id": todo_id,
                    "updates": {"last_result": result.get("result", "")},
                },
            )

        return {
            "last_agent_result": result.get("result", ""),
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


def _build_multi_agent_graph() -> StateGraph:
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

    # Graph topology
    builder.add_edge(START, "designer")
    builder.add_edge("designer", "planner")
    builder.add_edge("planner", "supervisor")

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
    builder.add_edge("input_required", "supervisor")
    builder.add_edge("completed", END)
    builder.add_edge("rejected", END)
    builder.add_edge("failed", END)

    return builder


# ========================================
# Build the selected graph
# ========================================

if GRAPH_MODE == "multi_agent":
    LOGGER.info("Using multi-agent graph topology")
    GRAPH = _build_multi_agent_graph().compile(checkpointer=CHECKPOINTER)
else:
    LOGGER.info("Using legacy graph topology")
    GRAPH = _build_legacy_graph().compile(checkpointer=CHECKPOINTER)


# ========================================
# FastAPI app
# ========================================

app = FastAPI(title="PentAGI LangGraph Orchestrator", version="0.2.0")


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
    return {"status": "ok", "graph_mode": GRAPH_MODE}


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
