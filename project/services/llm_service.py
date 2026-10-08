"""
Thin client for a local Ollama server running Qwen3 (default `qwen3:8b`).

Nothing here ever leaves your network: the model runs in the `ollama` container
(see docker-compose.yml) and this module only talks to it over HTTP.

chat_stream() yields plain dict events so the agent (services/chat_agent.py)
doesn't need to know Ollama's wire format:
    {"type": "thinking", "text": "..."}   model reasoning (only when think=True)
    {"type": "token",    "text": "..."}   answer text
    {"type": "tool_calls", "calls": [{"name": str, "arguments": dict}, ...]}
"""
import json

import requests


class LLMUnavailable(Exception):
    """Ollama isn't reachable, or the model isn't pulled."""


class ThinkTagSplitter:
    """Older Ollama builds put Qwen3's reasoning inline as <think>...</think> in
    the content stream instead of in a separate `thinking` field. This splits
    a chunked stream into ("thinking", text) / ("token", text) parts, correctly
    handling tags that arrive split across chunks."""

    OPEN, CLOSE = "<think>", "</think>"

    def __init__(self):
        self._buf = ""
        self._in_think = False

    def feed(self, chunk):
        self._buf += chunk
        out = []
        while True:
            tag = self.CLOSE if self._in_think else self.OPEN
            idx = self._buf.find(tag)
            if idx != -1:
                if idx:
                    out.append(("thinking" if self._in_think else "token", self._buf[:idx]))
                self._buf = self._buf[idx + len(tag):]
                self._in_think = not self._in_think
                continue
            # No full tag yet: emit everything except a tail that could be the start of one.
            keep = 0
            for k in range(min(len(tag) - 1, len(self._buf)), 0, -1):
                if tag.startswith(self._buf[-k:]):
                    keep = k
                    break
            emit, self._buf = self._buf[: len(self._buf) - keep], self._buf[len(self._buf) - keep:]
            if emit:
                out.append(("thinking" if self._in_think else "token", emit))
            return out

    def flush(self):
        out = [("thinking" if self._in_think else "token", self._buf)] if self._buf else []
        self._buf = ""
        return out


class LLMService:
    def __init__(self, app_config):
        self.base_url = app_config.LLM_BASE_URL.rstrip("/")
        self.model = app_config.LLM_MODEL
        self.num_ctx = app_config.LLM_NUM_CTX
        self.temperature = app_config.LLM_TEMPERATURE
        self.timeout = app_config.LLM_TIMEOUT_SECONDS
        self.keep_alive = app_config.LLM_KEEP_ALIVE

    def health(self):
        """{"available": bool, "model": str, "message": str}"""
        try:
            r = requests.get(f"{self.base_url}/api/tags", timeout=4)
            r.raise_for_status()
            names = [m.get("name", "") for m in r.json().get("models", [])]
        except Exception:
            return {"available": False, "model": self.model,
                    "message": "The AI service (Ollama) isn't reachable right now."}
        wanted = self.model if ":" in self.model else self.model + ":latest"
        if wanted not in names:
            return {"available": False, "model": self.model,
                    "message": f"Model '{self.model}' isn't downloaded yet. Run: docker compose exec ollama ollama pull {self.model}"}
        return {"available": True, "model": self.model, "message": "ready"}

    def chat_stream(self, messages, tools=None, think=False):
        payload = {
            "model": self.model,
            "messages": messages,
            "stream": True,
            "think": bool(think),
            "keep_alive": self.keep_alive,
            "options": {"num_ctx": self.num_ctx, "temperature": self.temperature},
        }
        if tools:
            payload["tools"] = tools
        try:
            resp = requests.post(f"{self.base_url}/api/chat", json=payload, stream=True,
                                 timeout=(5, self.timeout))
        except requests.RequestException:
            raise LLMUnavailable("The AI service (Ollama) isn't reachable right now.") from None
        try:
            if resp.status_code == 404:
                raise LLMUnavailable(f"Model '{self.model}' isn't downloaded yet on the AI server.")
            if resp.status_code != 200:
                raise LLMUnavailable(f"The AI service returned an error ({resp.status_code}).")
            splitter = ThinkTagSplitter()
            calls = []
            for line in resp.iter_lines():
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except ValueError:
                    continue
                if obj.get("error"):
                    raise LLMUnavailable(f"The AI service reported: {str(obj['error'])[:200]}")
                msg = obj.get("message") or {}
                if msg.get("thinking"):
                    yield {"type": "thinking", "text": msg["thinking"]}
                if msg.get("content"):
                    for kind, text in splitter.feed(msg["content"]):
                        yield {"type": kind, "text": text}
                for tc in msg.get("tool_calls") or []:
                    fn = tc.get("function") or {}
                    args = fn.get("arguments") or {}
                    if isinstance(args, str):  # some builds return a JSON string
                        try:
                            args = json.loads(args)
                        except ValueError:
                            args = {}
                    calls.append({"name": fn.get("name", ""), "arguments": args if isinstance(args, dict) else {}})
                if obj.get("done"):
                    break
            for kind, text in splitter.flush():
                yield {"type": kind, "text": text}
            if calls:
                yield {"type": "tool_calls", "calls": calls}
        except requests.RequestException:
            raise LLMUnavailable("Lost connection to the AI service while it was answering.") from None
        finally:
            resp.close()
