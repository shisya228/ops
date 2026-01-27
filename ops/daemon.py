from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .canonical import append_event
from .config import OpsConfig, load_config
from .db import connect, init_db
from .errors import DatabaseError, IOError
from .events import dedupe_key_from_draft, event_hash
from .lock import FileLock
from .utils import generate_ulid, iso_now

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8840


class OpsdServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], handler_cls: type[BaseHTTPRequestHandler]):
        super().__init__(server_address, handler_cls)
        self.config: OpsConfig
        self.paths: dict[str, Path]
        self.write_lock = threading.Lock()
        self.instance_lock: FileLock | None = None


class OpsdHandler(BaseHTTPRequestHandler):
    server: OpsdServer

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/health":
            self._send_json(404, {"error": "Not found"})
            return
        payload = {
            "ok": True,
            "version": "0.1",
            "workspace": str(self.server.paths["workspace"]),
            "pid": os.getpid(),
        }
        self._send_json(200, payload)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/events:batch":
            self._send_json(404, {"error": "Not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_json(400, {"error": "Invalid Content-Length"})
            return
        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json(400, {"error": "Invalid JSON"})
            return
        events = payload.get("events")
        if not isinstance(events, list):
            self._send_json(400, {"error": "events must be a list"})
            return
        with self.server.write_lock:
            response = self._handle_batch(events)
        self._send_json(200, response)

    def _handle_batch(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        inserted = 0
        skipped = 0
        failed = 0
        results: list[dict[str, Any]] = []
        conn = connect(self.server.paths["db"])
        created_at = iso_now(self.server.config.timezone)
        for draft in events:
            result: dict[str, Any] = {}
            error = self._validate_draft(draft)
            if error:
                failed += 1
                result["status"] = "failed"
                result["error"] = error
                results.append(result)
                continue
            dedupe_key = dedupe_key_from_draft(draft)
            if not dedupe_key:
                failed += 1
                result["status"] = "failed"
                result["error"] = "Unable to compute dedupe_key"
                results.append(result)
                continue
            row = conn.execute(
                "SELECT event_id FROM dedupe WHERE dedupe_key = ?",
                (dedupe_key,),
            ).fetchone()
            if row:
                skipped += 1
                results.append(
                    {
                        "status": "skipped",
                        "existing_event_id": row["event_id"],
                        "dedupe_key": dedupe_key,
                    }
                )
                continue
            event_core = {
                "schema_version": draft["schema_version"],
                "ts": draft["ts"],
                "type": draft["type"],
                "source": draft["source"],
                "refs": draft["refs"],
                "tags": draft.get("tags", []),
                "text": draft["text"],
                "payload": draft["payload"],
            }
            event = {
                **event_core,
                "id": generate_ulid(),
                "hash": event_hash(event_core),
                "dedupe_key": dedupe_key,
            }
            try:
                append_event(self.server.paths["events"], event)
            except IOError as exc:
                failed += 1
                results.append(
                    {
                        "status": "failed",
                        "error": str(exc),
                        "dedupe_key": dedupe_key,
                    }
                )
                continue
            try:
                with conn:
                    _insert_event(conn, event, dedupe_key, created_at)
            except DatabaseError as exc:
                failed += 1
                results.append(
                    {
                        "status": "failed",
                        "error": str(exc),
                        "dedupe_key": dedupe_key,
                    }
                )
                continue
            inserted += 1
            results.append(
                {
                    "status": "inserted",
                    "event_id": event["id"],
                    "dedupe_key": dedupe_key,
                    "hash": event["hash"]["value"],
                }
            )
        conn.close()
        return {
            "inserted": inserted,
            "skipped": skipped,
            "failed": failed,
            "results": results,
        }

    def _validate_draft(self, draft: dict[str, Any]) -> str | None:
        if not isinstance(draft, dict):
            return "Event draft must be an object"
        required = ["schema_version", "ts", "type", "source", "refs", "text", "payload"]
        for key in required:
            if key not in draft:
                return f"Missing {key}"
        if not isinstance(draft.get("source"), dict):
            return "source must be an object"
        if not draft["source"].get("kind") or not draft["source"].get("locator"):
            return "source.kind and source.locator are required"
        if not isinstance(draft.get("refs"), list):
            return "refs must be a list"
        if not isinstance(draft.get("text"), str):
            return "text must be a string"
        if not isinstance(draft.get("payload"), dict):
            return "payload must be an object"
        return None

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def _workspace_paths(config: OpsConfig) -> dict[str, Path]:
    workspace = config.workspace.resolve()
    return {
        "workspace": workspace,
        "raw": workspace / "raw" / "chat_json",
        "canonical": workspace / "canonical",
        "events": workspace / "canonical" / "events.jsonl",
        "index": workspace / "index",
        "db": workspace / "index" / "brain.sqlite",
    }


def _insert_event(conn, event: dict[str, Any], dedupe_key: str | None, created_at: str) -> None:
    conn.execute(
        """
        INSERT INTO events (
            id, schema_version, ts, type, tags_json, text, payload_json,
            source_kind, source_locator, source_meta_json, hash_algo, hash_value,
            dedupe_key, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event["id"],
            event["schema_version"],
            event["ts"],
            event["type"],
            json.dumps(event.get("tags", []), ensure_ascii=False),
            event["text"],
            json.dumps(event["payload"], ensure_ascii=False),
            event["source"]["kind"],
            event["source"]["locator"],
            json.dumps(event["source"].get("meta", {}), ensure_ascii=False),
            event["hash"]["algo"],
            event["hash"]["value"],
            dedupe_key,
            created_at,
        ),
    )
    for ref in event["refs"]:
        conn.execute(
            """
            INSERT INTO refs (event_id, ref_kind, uri, span_json, digest_algo, digest_value)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event["id"],
                ref["kind"],
                ref["uri"],
                json.dumps(ref.get("span", {}), ensure_ascii=False),
                (ref.get("digest") or {}).get("algo"),
                (ref.get("digest") or {}).get("value"),
            ),
        )
    if dedupe_key:
        conn.execute(
            "INSERT OR IGNORE INTO dedupe (dedupe_key, event_id, first_seen_ts) VALUES (?, ?, ?)",
            (dedupe_key, event["id"], event["ts"]),
        )


def run_opsd(host: str | None = None, port: int | None = None) -> None:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    paths["canonical"].mkdir(parents=True, exist_ok=True)
    paths["index"].mkdir(parents=True, exist_ok=True)
    if not paths["events"].exists():
        paths["events"].write_text("", encoding="utf-8")
    init_db(paths["db"])

    bind_host = host or DEFAULT_HOST
    bind_port = port if port is not None else int(os.environ.get("OPSD_PORT", DEFAULT_PORT))

    server = OpsdServer((bind_host, bind_port), OpsdHandler)
    server.config = config
    server.paths = paths
    lock_path = paths["canonical"] / ".opsd.lock"
    server.instance_lock = FileLock(lock_path, timeout=0)
    try:
        server.instance_lock.acquire()
    except IOError as exc:
        raise IOError(f"Failed to acquire opsd lock: {exc}") from exc

    if server.instance_lock._handle:
        server.instance_lock._handle.seek(0)
        server.instance_lock._handle.truncate()
        server.instance_lock._handle.write(f"pid={os.getpid()}\n")
        server.instance_lock._handle.flush()

    actual_port = server.server_address[1]
    print(f"opsd listening on http://{bind_host}:{actual_port}")
    try:
        server.serve_forever()
    finally:
        server.server_close()
        if server.instance_lock:
            server.instance_lock.release()
