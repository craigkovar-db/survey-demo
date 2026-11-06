"""In-process client over Genie One (Databricks' workspace-wide data chat).

Genie One is backed by the `onechat` service. Same backend powers:
  - the `databricks genie ask` CLI
  - the Genie One MCP server
  - the data-rooms REST endpoint

Endpoint: `POST /api/2.0/data-rooms/tools/onechat/responses`

Request:
    {
      "conversationId": "<optional, pass to continue>",
      "enable_viz": false,
      "input": [{"role":"user","type":"message",
                 "content":[{"type":"input_text","text":"<question>"}]}],
      "source": "databricks_webapp"
    }

Response: Server-Sent Events (`text/event-stream`), one `data: {...}` line per
event. We buffer the whole stream and look at the final `type: "response.done"`
event, which carries the completed response in `.response.output[]`:

  - items with `type: "function_call"` and `name: "execute_sql_query"` →
    tool_calls (the SQL the agent ran)
  - items with `type: "message"`, `role: "assistant"`, and
    `metadata.ui_type == "FINAL_RESPONSE"` → the markdown answer

Auth modes (both supported):
  - OBO (production on Databricks Apps): pass `user_token` and we send
    `Authorization: Bearer <user_token>` so Genie answers with the END USER's
    UC permissions. Only data they can see shows up.
  - App/profile (local dev): `user_token` is None and we fall back to
    `WorkspaceClient().config.authenticate()`.
"""
from __future__ import annotations

import json
import os
from typing import Any, Iterator

import requests
from databricks.sdk import WorkspaceClient

GENIE_ONE_PATH = "/api/2.0/data-rooms/tools/onechat/responses"
GENIE_ONE_TIMEOUT = int(os.environ.get("GENIE_ONE_TIMEOUT", "180"))

_w: WorkspaceClient | None = None


def _workspace() -> WorkspaceClient:
    """Default (app-auth) WorkspaceClient. Only used when no OBO token is
    passed — a user-scoped client is built per-request in `_auth`."""
    global _w
    if _w is None:
        _w = WorkspaceClient()
    return _w


def _auth(user_token: str | None) -> tuple[dict[str, str], str]:
    """Return (headers, host) for the onechat REST call.

    OBO when `user_token` is set — the single Authorization header is enough
    and we take the host from env (`DATABRICKS_HOST` is always set on
    Databricks Apps). Otherwise fall back to the SDK's `config.authenticate()`
    which honors profile/SP credentials.
    """
    if user_token:
        host = (os.environ.get("DATABRICKS_HOST") or "").rstrip("/")
        return ({"Authorization": f"Bearer {user_token}"}, host)
    cfg = _workspace().config
    return (cfg.authenticate(), (cfg.host or "").rstrip("/"))


def _parse_events(sse_body: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line in sse_body.split("\n"):
        if not line.startswith("data:"):
            continue
        payload = line[len("data:"):].strip()
        if not payload:
            continue
        try:
            events.append(json.loads(payload))
        except json.JSONDecodeError:
            # ignore malformed events — the stream can legitimately include heartbeats etc.
            continue
    return events


def _synthesize(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Walk the event stream and build {status, conversation_id, text, tool_calls}."""
    # The final completed state is on the last `response.done` event.
    done = next(
        (e for e in reversed(events) if e.get("type") == "response.done"),
        None,
    )
    if done is None:
        # Stream closed before a done event — surface whatever we got.
        return {
            "status": "failed",
            "conversation_id": None,
            "text": "Genie One did not return a completed response.",
            "tool_calls": [],
        }

    response = done.get("response") or {}
    conversation_id = done.get("conversation_id") or response.get("conversation_id")

    final_text_parts: list[str] = []
    tool_calls: list[dict[str, Any]] = []

    for item in response.get("output") or []:
        itype = item.get("type")

        if itype == "message":
            # ui_type == FINAL_RESPONSE is the user-facing markdown answer.
            # ui_type == THOUGHT is the model's internal reasoning — skip it.
            ui_type = (item.get("metadata") or {}).get("ui_type") or ""
            if ui_type != "FINAL_RESPONSE":
                continue
            for part in item.get("content") or []:
                if part.get("type") == "output_text":
                    txt = part.get("text") or ""
                    if txt:
                        final_text_parts.append(txt)

        elif itype == "function_call" and item.get("name") == "execute_sql_query":
            # Normalize to the shape the CLI's --output json produces.
            args_raw = item.get("arguments") or "{}"
            try:
                args = json.loads(args_raw)
            except json.JSONDecodeError:
                args = {}
            tool_calls.append(
                {
                    "name": "execute_sql_query",
                    "sql": args.get("query") or "",
                    "arguments": args_raw,
                }
            )

    return {
        "status": response.get("status") or "completed",
        "conversation_id": conversation_id,
        "text": "\n\n".join(final_text_parts).strip(),
        "tool_calls": tool_calls,
    }


def ask(
    question: str,
    conversation_id: str | None = None,
    user_token: str | None = None,
) -> dict[str, Any]:
    """Ask Genie One. Pass `conversation_id` from a prior answer to continue.

    When `user_token` is provided (OBO), the call is authorized as the end
    user, so Genie's data access respects their UC permissions.
    """
    q = (question or "").strip()
    if not q:
        return {
            "status": "failed",
            "conversation_id": None,
            "text": "Empty question.",
            "tool_calls": [],
        }

    headers, host = _auth(user_token)
    headers["Content-Type"] = "application/json"
    headers["Accept"] = "text/event-stream"

    body: dict[str, Any] = {
        "enable_viz": False,
        "input": [
            {
                "content": [{"type": "input_text", "text": q}],
                "role": "user",
                "type": "message",
            }
        ],
        "source": "databricks_webapp",
    }
    if conversation_id:
        body["conversationId"] = conversation_id

    resp = requests.post(
        host + GENIE_ONE_PATH,
        headers=headers,
        json=body,
        timeout=GENIE_ONE_TIMEOUT,
        stream=False,
    )
    if not resp.ok:
        return {
            "status": "failed",
            "conversation_id": None,
            "text": f"Genie One returned {resp.status_code}: {resp.text[:500]}",
            "tool_calls": [],
        }

    events = _parse_events(resp.text or "")
    return _synthesize(events)


# ---------- Streaming (SSE) variant ----------

def _sse(data: dict[str, Any]) -> str:
    return f"data: {json.dumps(data)}\n\n"


def _progress_from_event(ev: dict[str, Any]) -> dict[str, Any] | None:
    """Translate one onechat SSE event into a friendly progress hint for the UI.

    We only surface events that are useful to a human watching the chat
    (model thoughts, SQL executions, asset inspections). Noise events
    (`.added` deltas, episode fetches, raw function_call_output rows) are
    skipped. Returns None when nothing user-visible happened.
    """
    etype = ev.get("type") or ""
    item = ev.get("item") or {}
    if not isinstance(item, dict):
        return None

    # Items get their intent populated when they're `added`; `.done` only
    # confirms completion (and for most function calls isn't emitted at all).
    # So we key off `.added`.
    if etype != "response.output_item.added":
        return None

    itype = item.get("type")

    if itype == "message":
        meta = item.get("metadata") or {}
        if meta.get("ui_type") != "THOUGHT":
            return None  # final response text is handled separately
        parts = item.get("content") or []
        for p in parts:
            if p.get("type") == "output_text" and p.get("text"):
                return {"type": "progress", "kind": "thought", "text": p["text"]}
        return None

    if itype == "function_call":
        name = item.get("name") or ""
        meta = item.get("metadata") or {}
        args_raw = item.get("arguments") or "{}"
        try:
            args = json.loads(args_raw)
        except json.JSONDecodeError:
            args = {}

        if name == "execute_sql_query":
            sql = args.get("query") or ""
            return {"type": "progress", "kind": "sql", "sql": sql}
        if name == "get_asset_info":
            asset = args.get("asset") or meta.get("identifier") or "(asset)"
            # Trim leading `table.`/`view.` kind prefix the API uses.
            if asset.count(".") >= 3 and asset.split(".", 1)[0] in {"table", "view", "schema", "catalog"}:
                asset = asset.split(".", 1)[1]
            return {
                "type": "progress",
                "kind": "inspect",
                "text": f"Inspecting {asset}",
            }
        if name == "episode_fetch":
            return None  # internal conversation memory lookup — not useful
        # Generic fallback for other tool calls — show the name so the user
        # at least knows something is happening.
        return {
            "type": "progress",
            "kind": "tool",
            "text": f"Running {name}",
        }

    return None


def ask_stream(
    question: str,
    conversation_id: str | None = None,
    user_token: str | None = None,
) -> Iterator[str]:
    """Streaming version of `ask`. Yields SSE events the frontend consumes:

      {"type": "progress", "kind": "thought"|"sql"|"inspect"|"tool", ...}
      {"type": "complete", "answer": {status, conversation_id, text, tool_calls}}
      {"type": "error",    "message": "..."}

    Same OBO behavior as `ask`: with `user_token`, the call is authorized as
    the end user.
    """
    q = (question or "").strip()
    if not q:
        yield _sse({"type": "error", "message": "Empty question."})
        return

    headers, host = _auth(user_token)
    headers["Content-Type"] = "application/json"
    headers["Accept"] = "text/event-stream"

    body: dict[str, Any] = {
        "enable_viz": False,
        "input": [
            {
                "content": [{"type": "input_text", "text": q}],
                "role": "user",
                "type": "message",
            }
        ],
        "source": "databricks_webapp",
    }
    if conversation_id:
        body["conversationId"] = conversation_id

    try:
        resp = requests.post(
            host + GENIE_ONE_PATH,
            headers=headers,
            json=body,
            timeout=GENIE_ONE_TIMEOUT,
            stream=True,
        )
        if not resp.ok:
            yield _sse({
                "type": "error",
                "message": f"Genie One returned {resp.status_code}: {resp.text[:500]}",
            })
            return

        collected: list[dict[str, Any]] = []
        for raw_line in resp.iter_lines(decode_unicode=True):
            if not raw_line or not raw_line.startswith("data:"):
                continue
            payload = raw_line[len("data:"):].strip()
            if not payload:
                continue
            try:
                event = json.loads(payload)
            except json.JSONDecodeError:
                continue
            collected.append(event)

            prog = _progress_from_event(event)
            if prog is not None:
                yield _sse(prog)

        # Final synthesized answer from the whole stream.
        yield _sse({"type": "complete", "answer": _synthesize(collected)})

    except requests.exceptions.RequestException as e:
        yield _sse({"type": "error", "message": f"{type(e).__name__}: {e}"})
