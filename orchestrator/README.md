# PentAgentX LangGraph Orchestrator

This service provides the phase-1 Python LangGraph orchestration layer for PentAgentX.

## Environment

- `PENTAGENTX_INTERNAL_BASE_URL`: Go internal orchestrator API base URL
- `PENTAGENTX_INTERNAL_VERIFY_SSL`: whether to verify the Go HTTPS certificate, default `true`
- `LANGGRAPH_INTERNAL_TOKEN`: shared secret for both Go and Python internal calls
- `LANGGRAPH_CHECKPOINT_PATH`: SQLite checkpoint file path
- `LANGGRAPH_HOST`: bind host, default `0.0.0.0`
- `LANGGRAPH_PORT`: bind port, default `8091`

## Run

```bash
pip install -r requirements.txt
python app.py
```

## Docker

```bash
docker build -t pentagentx-langgraph-orchestrator ./orchestrator
```
