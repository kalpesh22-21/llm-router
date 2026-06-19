"""
Tiny OpenAI-compatible mock upstream for the discovery E2E.

  GET  /health              → 200 (container healthcheck)
  GET  /v1/models           → 200 (so the discovery probe succeeds)
  POST /v1/chat/completions → 200 with a canned completion + usage

To simulate the model going DOWN/UP, stop/start this container:
  docker compose -f docker-compose.e2e.yml stop  mock-upstream
  docker compose -f docker-compose.e2e.yml start mock-upstream
"""

from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="mock-upstream")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/v1/models")
async def models():
    return {
        "object": "list",
        "data": [{"id": "test-model", "object": "model", "owned_by": "mock"}],
    }


@app.post("/v1/chat/completions")
async def chat_completions():
    return JSONResponse(
        {
            "id": "chatcmpl-mock",
            "object": "chat.completion",
            "model": "test-model",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "pong from mock upstream",
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 5, "completion_tokens": 4, "total_tokens": 9},
        }
    )
