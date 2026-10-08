"""
AI assistant endpoints.

  GET  /api/chat/status  -> is the local model reachable / downloaded?
  POST /api/chat         -> streams the assistant's answer as NDJSON events
                            (one JSON object per line; see services/chat_agent.py)

The server keeps NO conversation state: the browser resends the recent turns
each time. That keeps it correct behind nginx's three app replicas without
sticky sessions. Only "user"/"assistant" text turns are accepted from the
client — it can't inject "system" or "tool" messages.
"""
import json

from flask import Blueprint, Response, jsonify, request, stream_with_context

from routes._common import catalog, config, llm, login_required, postgres
from services.chat_agent import ChatAgent
from services.rate_limiter import limiter

bp = Blueprint("chat", __name__)

MAX_MESSAGE_CHARS = 4000


@bp.get("/chat/status")
@login_required
def chat_status(user):
    return jsonify(llm().health())


def _clean_history(raw, max_messages):
    if not isinstance(raw, list):
        return None
    out = []
    for m in raw[-max_messages:]:
        if not isinstance(m, dict) or m.get("role") not in ("user", "assistant"):
            continue
        content = m.get("content")
        if not isinstance(content, str) or not content.strip():
            continue
        out.append({"role": m["role"], "content": content[:MAX_MESSAGE_CHARS]})
    while out and out[0]["role"] != "user":  # must start with a user turn
        out.pop(0)
    if not out or out[-1]["role"] != "user":
        return None
    return out


@bp.post("/chat")
@login_required
@limiter.limit(lambda: config().CHAT_RATE_LIMIT)
def chat(user):
    data = request.get_json(silent=True) or {}
    history = _clean_history(data.get("messages"), config().CHAT_HISTORY_MESSAGES)
    if history is None:
        return jsonify({"error": "Send a message to start."}), 400

    def audit(detail, row_count, dataset_ids, action="ai_sql"):
        details = ({"sql": detail[:2000], "rows": row_count} if action == "ai_sql"
                   else {"tool": detail[:600]})
        postgres().log_action(user["id"], user["username"], action,
                              ",".join(dataset_ids) or None, details)

    agent = ChatAgent(llm(), catalog(), postgres(), config(), user,
                      think=bool(data.get("think")), audit=audit)

    def generate():
        try:
            for event in agent.run(history):
                yield json.dumps(event, default=str) + "\n"
        except Exception:
            yield json.dumps({"type": "error", "message": "Something went wrong while answering. Please try again."}) + "\n"

    return Response(stream_with_context(generate()), mimetype="application/x-ndjson",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
