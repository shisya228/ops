from __future__ import annotations

import json
import os
import shutil
import threading
from collections import Counter
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse
from zoneinfo import ZoneInfo

from . import adapters
from .canonical import append_event
from .config import OpsConfig, load_config
from .db import SCHEMA_VERSION, connect, init_db
from .errors import DatabaseError, IOError
from .events import dedupe_key_from_draft, dedupe_key_from_event, event_hash
from .lock import FileLock
from .utils import generate_ulid, iso_from_timestamp, iso_now, sha256_hex

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7777


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
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        if path == "/health":
            payload = {
                "ok": True,
                "version": "0.2",
                "schema_version": SCHEMA_VERSION,
            }
            self._send_json(200, payload)
            return
        if path == "/v1/events":
            self._handle_events_list(query)
            return
        if path.startswith("/v1/events/"):
            event_id = unquote(path.split("/v1/events/", 1)[1])
            self._handle_event_show(event_id)
            return
        if path == "/v1/sources":
            self._handle_sources_list()
            return
        if path.startswith("/v1/sources/"):
            name = unquote(path.split("/v1/sources/", 1)[1])
            self._handle_source_show(name)
            return
        if path == "/v1/views":
            self._handle_views_list()
            return
        if path.startswith("/v1/views/"):
            name = unquote(path.split("/v1/views/", 1)[1])
            self._handle_view_show(name)
            return
        if path == "/v1/jobs":
            self._handle_jobs_list()
            return
        if path.startswith("/v1/jobs/"):
            name = unquote(path.split("/v1/jobs/", 1)[1])
            if name.endswith("/runs"):
                name = name[: -len("/runs")]
                self._handle_job_runs(name)
                return
            self._handle_job_show(name)
            return
        if path == "/v1/artifacts":
            self._handle_artifacts_list(query)
            return
        self._send_json(404, {"error": "Not found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/v1/events:batch":
            payload = self._read_json()
            if payload is None:
                return
            events = payload.get("events")
            if not isinstance(events, list):
                self._send_json(400, {"error": "events must be a list"})
                return
            options = payload.get("options", {}) if isinstance(payload.get("options", {}), dict) else {}
            with self.server.write_lock:
                response = self._handle_batch(events, options)
            self._send_json(200, response)
            return
        if path == "/v1/sources":
            payload = self._read_json()
            if payload is None:
                return
            with self.server.write_lock:
                self._handle_source_create(payload)
            return
        if path.startswith("/v1/sources/") and path.endswith(":test"):
            name = unquote(path.split("/v1/sources/", 1)[1].rsplit(":test", 1)[0])
            with self.server.write_lock:
                self._handle_source_test(name)
            return
        if path.startswith("/v1/ingests/") and path.endswith(":run"):
            name = unquote(path.split("/v1/ingests/", 1)[1].rsplit(":run", 1)[0])
            payload = self._read_json()
            if payload is None:
                return
            with self.server.write_lock:
                self._handle_ingest_run(name, payload)
            return
        if path == "/v1/views":
            payload = self._read_json()
            if payload is None:
                return
            with self.server.write_lock:
                self._handle_view_create(payload)
            return
        if path.startswith("/v1/views/") and path.endswith(":query"):
            name = unquote(path.split("/v1/views/", 1)[1].rsplit(":query", 1)[0])
            payload = self._read_json()
            if payload is None:
                return
            self._handle_view_query(name, payload)
            return
        if path == "/v1/jobs":
            payload = self._read_json()
            if payload is None:
                return
            with self.server.write_lock:
                self._handle_job_create(payload)
            return
        if path.startswith("/v1/jobs/") and path.endswith(":run"):
            name = unquote(path.split("/v1/jobs/", 1)[1].rsplit(":run", 1)[0])
            payload = self._read_json() or {}
            with self.server.write_lock:
                self._handle_job_run(name, payload)
            return
        if path == "/v1/artifacts:pack":
            payload = self._read_json()
            if payload is None:
                return
            with self.server.write_lock:
                self._handle_artifact_pack(payload)
            return
        self._send_json(404, {"error": "Not found"})

    def do_DELETE(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if path.startswith("/v1/sources/"):
            name = unquote(path.split("/v1/sources/", 1)[1])
            with self.server.write_lock:
                self._handle_source_delete(name)
            return
        if path.startswith("/v1/views/"):
            name = unquote(path.split("/v1/views/", 1)[1])
            with self.server.write_lock:
                self._handle_view_delete(name)
            return
        if path.startswith("/v1/jobs/"):
            name = unquote(path.split("/v1/jobs/", 1)[1])
            with self.server.write_lock:
                self._handle_job_delete(name)
            return
        self._send_json(404, {"error": "Not found"})

    def _handle_batch(self, events: list[dict[str, Any]], options: dict[str, Any]) -> dict[str, Any]:
        dedupe = bool(options.get("dedupe", True))
        result = _ingest_drafts(
            self.server,
            events,
            dedupe=dedupe,
            dry_run=False,
        )
        response = {
            "inserted": result["new"],
            "skipped": result["skipped"],
            "failed": result["failed"],
            "results": result["results"],
            "new": result["new"],
            "errors": result["errors"],
            "ids": result["ids"],
        }
        return response

    def _handle_events_list(self, query: dict[str, list[str]]) -> None:
        conn = connect(self.server.paths["db"])
        params = _event_query_params(query)
        items = _query_events(conn, self.server.config, params)
        conn.close()
        self._send_json(200, {"items": items})

    def _handle_event_show(self, event_id: str) -> None:
        conn = connect(self.server.paths["db"])
        event = _fetch_event(conn, event_id)
        conn.close()
        if not event:
            self._send_json(404, {"error": "Event not found"})
            return
        self._send_json(200, event)

    def _handle_sources_list(self) -> None:
        conn = connect(self.server.paths["db"])
        rows = conn.execute("SELECT * FROM sources ORDER BY created_at DESC").fetchall()
        conn.close()
        items = [_source_from_row(row) for row in rows]
        self._send_json(200, {"items": items})

    def _handle_source_show(self, name: str) -> None:
        conn = connect(self.server.paths["db"])
        row = conn.execute("SELECT * FROM sources WHERE name = ?", (name,)).fetchone()
        conn.close()
        if not row:
            self._send_json(404, {"error": "Source not found"})
            return
        self._send_json(200, _source_from_row(row))

    def _handle_source_create(self, payload: dict[str, Any]) -> None:
        name = payload.get("name")
        kind = payload.get("kind")
        config = payload.get("config")
        tags = payload.get("tags", [])
        if not isinstance(name, str) or not name:
            self._send_json(400, {"error": "name is required"})
            return
        if not isinstance(kind, str) or not kind:
            self._send_json(400, {"error": "kind is required"})
            return
        if not isinstance(config, dict):
            self._send_json(400, {"error": "config must be an object"})
            return
        if not isinstance(tags, list):
            self._send_json(400, {"error": "tags must be a list"})
            return
        if kind != "chat_json_file":
            self._send_json(400, {"error": f"Unsupported source kind: {kind}"})
            return
        created_at = iso_now(self.server.config.timezone)
        conn = connect(self.server.paths["db"])
        try:
            with conn:
                conn.execute(
                    "INSERT INTO sources (name, kind, config_json, tags_json, created_at) VALUES (?, ?, ?, ?, ?)",
                    (
                        name,
                        kind,
                        json.dumps(_normalize_source_config(config), ensure_ascii=False),
                        json.dumps(tags, ensure_ascii=False),
                        created_at,
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            conn.close()
            self._send_json(400, {"error": f"Failed to create source: {exc}"})
            return
        conn.close()
        self._send_json(200, {"name": name, "kind": kind, "config": _normalize_source_config(config), "tags": tags, "created_at": created_at})

    def _handle_source_delete(self, name: str) -> None:
        conn = connect(self.server.paths["db"])
        with conn:
            conn.execute("DELETE FROM sources WHERE name = ?", (name,))
        conn.close()
        self._send_json(200, {"ok": True})

    def _handle_source_test(self, name: str) -> None:
        conn = connect(self.server.paths["db"])
        row = conn.execute("SELECT * FROM sources WHERE name = ?", (name,)).fetchone()
        conn.close()
        if not row:
            self._send_json(404, {"error": "Source not found"})
            return
        source = _source_from_row(row)
        ok, details, error = _test_source(source)
        if ok:
            self._send_json(200, {"ok": True, "details": details})
        else:
            self._send_json(200, {"ok": False, "error": error})

    def _handle_ingest_run(self, name: str, payload: dict[str, Any]) -> None:
        conn = connect(self.server.paths["db"])
        row = conn.execute("SELECT * FROM sources WHERE name = ?", (name,)).fetchone()
        conn.close()
        if not row:
            self._send_json(404, {"error": "Source not found"})
            return
        source = _source_from_row(row)
        extra_tags = payload.get("tags", [])
        if not isinstance(extra_tags, list):
            self._send_json(400, {"error": "tags must be a list"})
            return
        dry_run = bool(payload.get("dry_run", False))
        try:
            drafts = _build_source_drafts(self.server, source, extra_tags)
        except Exception as exc:  # noqa: BLE001
            self._send_json(400, {"error": str(exc)})
            return
        result = _ingest_drafts(
            self.server,
            drafts,
            dedupe=True,
            dry_run=dry_run,
        )
        self._send_json(200, {"new": result["new"], "skipped": result["skipped"], "failed": result["failed"], "errors": result["errors"]})

    def _handle_views_list(self) -> None:
        conn = connect(self.server.paths["db"])
        rows = conn.execute("SELECT * FROM views ORDER BY created_at DESC").fetchall()
        conn.close()
        items = [_view_from_row(row) for row in rows]
        self._send_json(200, {"items": items})

    def _handle_view_show(self, name: str) -> None:
        conn = connect(self.server.paths["db"])
        row = conn.execute("SELECT * FROM views WHERE name = ?", (name,)).fetchone()
        conn.close()
        if not row:
            self._send_json(404, {"error": "View not found"})
            return
        self._send_json(200, _view_from_row(row))

    def _handle_view_create(self, payload: dict[str, Any]) -> None:
        name = payload.get("name")
        description = payload.get("description", "")
        query = payload.get("query")
        if not isinstance(name, str) or not name:
            self._send_json(400, {"error": "name is required"})
            return
        if not isinstance(description, str):
            self._send_json(400, {"error": "description must be a string"})
            return
        if not isinstance(query, dict):
            self._send_json(400, {"error": "query must be an object"})
            return
        created_at = iso_now(self.server.config.timezone)
        conn = connect(self.server.paths["db"])
        try:
            with conn:
                conn.execute(
                    "INSERT INTO views (name, description, query_json, created_at) VALUES (?, ?, ?, ?)",
                    (name, description, json.dumps(query, ensure_ascii=False), created_at),
                )
        except Exception as exc:  # noqa: BLE001
            conn.close()
            self._send_json(400, {"error": f"Failed to create view: {exc}"})
            return
        conn.close()
        self._send_json(200, {"name": name, "description": description, "query": query, "created_at": created_at})

    def _handle_view_delete(self, name: str) -> None:
        conn = connect(self.server.paths["db"])
        with conn:
            conn.execute("DELETE FROM views WHERE name = ?", (name,))
        conn.close()
        self._send_json(200, {"ok": True})

    def _handle_view_query(self, name: str, payload: dict[str, Any]) -> None:
        conn = connect(self.server.paths["db"])
        row = conn.execute("SELECT * FROM views WHERE name = ?", (name,)).fetchone()
        if not row:
            conn.close()
            self._send_json(404, {"error": "View not found"})
            return
        view = _view_from_row(row)
        filters = payload.get("filters", {}) if isinstance(payload.get("filters", {}), dict) else {}
        limit = int(payload.get("limit", 50))
        merged_query = _merge_view_filters(view.get("query", {}), filters)
        params = {
            "q": None,
            "types": merged_query.get("filters", {}).get("type"),
            "tags": merged_query.get("filters", {}).get("tag"),
            "after": merged_query.get("filters", {}).get("after"),
            "before": merged_query.get("filters", {}).get("before"),
            "limit": limit,
            "format": "summary",
            "order": merged_query.get("order", "desc"),
        }
        items = _query_events(conn, self.server.config, params)
        conn.close()
        self._send_json(200, {"items": items})

    def _handle_jobs_list(self) -> None:
        conn = connect(self.server.paths["db"])
        rows = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()
        conn.close()
        items = [_job_from_row(row) for row in rows]
        self._send_json(200, {"items": items})

    def _handle_job_show(self, name: str) -> None:
        conn = connect(self.server.paths["db"])
        row = conn.execute("SELECT * FROM jobs WHERE name = ?", (name,)).fetchone()
        conn.close()
        if not row:
            self._send_json(404, {"error": "Job not found"})
            return
        self._send_json(200, _job_from_row(row))

    def _handle_job_runs(self, name: str) -> None:
        conn = connect(self.server.paths["db"])
        rows = conn.execute(
            "SELECT * FROM job_runs WHERE job_name = ? ORDER BY started_at DESC",
            (name,),
        ).fetchall()
        conn.close()
        items = [_job_run_from_row(row) for row in rows]
        self._send_json(200, {"items": items})

    def _handle_job_create(self, payload: dict[str, Any]) -> None:
        name = payload.get("name")
        kind = payload.get("kind")
        config = payload.get("config", {})
        enabled = payload.get("enabled", True)
        if not isinstance(name, str) or not name:
            self._send_json(400, {"error": "name is required"})
            return
        if not isinstance(kind, str) or not kind:
            self._send_json(400, {"error": "kind is required"})
            return
        if not isinstance(config, dict):
            self._send_json(400, {"error": "config must be an object"})
            return
        created_at = iso_now(self.server.config.timezone)
        conn = connect(self.server.paths["db"])
        try:
            with conn:
                conn.execute(
                    "INSERT INTO jobs (name, kind, config_json, enabled, created_at) VALUES (?, ?, ?, ?, ?)",
                    (name, kind, json.dumps(config, ensure_ascii=False), int(bool(enabled)), created_at),
                )
        except Exception as exc:  # noqa: BLE001
            conn.close()
            self._send_json(400, {"error": f"Failed to create job: {exc}"})
            return
        conn.close()
        self._send_json(200, {"name": name, "kind": kind, "config": config, "enabled": bool(enabled), "created_at": created_at})

    def _handle_job_delete(self, name: str) -> None:
        conn = connect(self.server.paths["db"])
        with conn:
            conn.execute("DELETE FROM jobs WHERE name = ?", (name,))
        conn.close()
        self._send_json(200, {"ok": True})

    def _handle_job_run(self, name: str, payload: dict[str, Any]) -> None:
        conn = connect(self.server.paths["db"])
        row = conn.execute("SELECT * FROM jobs WHERE name = ?", (name,)).fetchone()
        if not row:
            conn.close()
            self._send_json(404, {"error": "Job not found"})
            return
        job = _job_from_row(row)
        run_id = generate_ulid()
        started_at = iso_now(self.server.config.timezone)
        with conn:
            conn.execute(
                "INSERT INTO job_runs (id, job_name, started_at, status) VALUES (?, ?, ?, ?)",
                (run_id, name, started_at, "running"),
            )
        conn.close()
        status = "ok"
        output: dict[str, Any] = {}
        error = None
        try:
            output = _execute_job(self.server, job)
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            error = str(exc)
        finished_at = iso_now(self.server.config.timezone)
        conn = connect(self.server.paths["db"])
        with conn:
            conn.execute(
                "UPDATE job_runs SET finished_at = ?, status = ?, output_json = ?, error = ? WHERE id = ?",
                (finished_at, status, json.dumps(output, ensure_ascii=False), error, run_id),
            )
        conn.close()
        response = {"run_id": run_id, "status": status, "outputs": output}
        if status != "ok":
            response["error"] = error
        self._send_json(200, response)

    def _handle_artifacts_list(self, query: dict[str, list[str]]) -> None:
        conn = connect(self.server.paths["db"])
        params = _event_query_params(query)
        params["types"] = ["artifact.created"]
        params["format"] = "full"
        events = _query_events(conn, self.server.config, params)
        items = [_artifact_from_event(event) for event in events]
        conn.close()
        self._send_json(200, {"items": items})

    def _handle_artifact_pack(self, payload: dict[str, Any]) -> None:
        tag = payload.get("tag")
        out_dir = payload.get("out_dir")
        if not isinstance(tag, str) or not tag:
            self._send_json(400, {"error": "tag is required"})
            return
        if not isinstance(out_dir, str) or not out_dir:
            self._send_json(400, {"error": "out_dir is required"})
            return
        output = _run_artifact_pack(self.server, tag=tag, out_dir=out_dir)
        self._send_json(200, output)

    def _read_json(self) -> dict[str, Any] | None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_json(400, {"error": "Invalid Content-Length"})
            return None
        body = self.rfile.read(length)
        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json(400, {"error": "Invalid JSON"})
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
        "artifacts": workspace / "artifacts",
    }


def _ensure_workspace(paths: dict[str, Path]) -> None:
    paths["raw"].mkdir(parents=True, exist_ok=True)
    paths["canonical"].mkdir(parents=True, exist_ok=True)
    paths["index"].mkdir(parents=True, exist_ok=True)
    paths["artifacts"].mkdir(parents=True, exist_ok=True)
    if not paths["events"].exists():
        paths["events"].write_text("", encoding="utf-8")


def _normalize_source_config(config: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(config)
    if "copy" not in normalized:
        normalized["copy"] = True
    return normalized


def _source_from_row(row) -> dict[str, Any]:
    return {
        "name": row["name"],
        "kind": row["kind"],
        "config": json.loads(row["config_json"]),
        "tags": json.loads(row["tags_json"]),
        "created_at": row["created_at"],
    }


def _view_from_row(row) -> dict[str, Any]:
    return {
        "name": row["name"],
        "description": row["description"],
        "query": json.loads(row["query_json"]),
        "created_at": row["created_at"],
    }


def _job_from_row(row) -> dict[str, Any]:
    return {
        "name": row["name"],
        "kind": row["kind"],
        "config": json.loads(row["config_json"]),
        "enabled": bool(row["enabled"]),
        "created_at": row["created_at"],
    }


def _job_run_from_row(row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "job_name": row["job_name"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "status": row["status"],
        "outputs": json.loads(row["output_json"]),
        "error": row["error"],
    }


def _test_source(source: dict[str, Any]) -> tuple[bool, dict[str, Any], str | None]:
    config = source.get("config", {})
    path_value = config.get("path")
    if not isinstance(path_value, str) or not path_value:
        return False, {}, "config.path is required"
    path = Path(path_value)
    if not path.is_absolute():
        path = Path.cwd() / path
    if not path.exists():
        return False, {}, f"Path does not exist: {path}"
    if not path.is_file():
        return False, {}, f"Path is not a file: {path}"
    try:
        size = path.stat().st_size
    except OSError as exc:
        return False, {}, f"Unable to read file: {exc}"
    return True, {"path": str(path), "size": size}, None


def _build_source_drafts(server: OpsdServer, source: dict[str, Any], extra_tags: list[str]) -> list[dict[str, Any]]:
    config = _normalize_source_config(source.get("config", {}))
    path_value = config.get("path")
    if not isinstance(path_value, str) or not path_value:
        raise ValueError("config.path is required")
    source_path = Path(path_value)
    if not source_path.is_absolute():
        source_path = Path.cwd() / source_path
    locator_path = source_path
    locator_value = str(locator_path)
    if bool(config.get("copy", True)):
        locator_path = _copy_source(source_path, server.paths["raw"])
        locator_value = str(locator_path)
    tags = _merge_tags(source.get("tags", []), extra_tags)
    drafts = []
    for idx, message in enumerate(adapters.iter_chat_messages(locator_path)):
        content = message.get("content")
        if content is None:
            raise ValueError(f"Missing content at idx {idx}")
        ts_value = message.get("ts")
        if not ts_value:
            ts_value = iso_from_timestamp(locator_path.stat().st_mtime, server.config.timezone)
        text = content.replace("\r\n", "\n").replace("\r", "\n")
        payload = {"speaker": message.get("speaker"), "content": content}
        if message.get("thread_id") is not None:
            payload["thread_id"] = message.get("thread_id")
        draft = {
            "schema_version": "0.2",
            "ts": ts_value,
            "type": "chat.message",
            "source": {"kind": source.get("kind"), "locator": locator_value, "meta": {}},
            "refs": [{"kind": "file", "uri": f"file:{locator_value}", "span": {"idx": idx}}],
            "tags": tags,
            "text": text,
            "payload": payload,
        }
        drafts.append(draft)
    return drafts


def _merge_tags(base: list[str], extra: list[str]) -> list[str]:
    merged: list[str] = []
    for item in base + extra:
        if item not in merged:
            merged.append(item)
    return merged


def _copy_source(path: Path, dest_dir: Path) -> Path:
    digest = sha256_hex(path.read_bytes())
    dest = dest_dir / f"{digest[:12]}_{path.name}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, dest)
    return dest


def _ingest_drafts(server: OpsdServer, drafts: list[dict[str, Any]], dedupe: bool, dry_run: bool) -> dict[str, Any]:
    inserted = 0
    skipped = 0
    failed = 0
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    ids: list[str] = []
    conn = connect(server.paths["db"])
    created_at = iso_now(server.config.timezone)
    for draft in drafts:
        result: dict[str, Any] = {}
        error = _validate_draft(draft)
        if error:
            failed += 1
            errors.append(error)
            result["status"] = "failed"
            result["error"] = error
            results.append(result)
            continue
        dedupe_key = dedupe_key_from_draft(draft)
        if dedupe and not dedupe_key:
            failed += 1
            error = "Unable to compute dedupe_key"
            errors.append(error)
            result["status"] = "failed"
            result["error"] = error
            results.append(result)
            continue
        if dedupe and dedupe_key:
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
        if not dry_run:
            try:
                append_event(server.paths["events"], event)
            except IOError as exc:
                failed += 1
                errors.append(str(exc))
                results.append({"status": "failed", "error": str(exc), "dedupe_key": dedupe_key})
                continue
            try:
                with conn:
                    _insert_event(conn, event, dedupe_key, created_at)
            except DatabaseError as exc:
                failed += 1
                errors.append(str(exc))
                results.append({"status": "failed", "error": str(exc), "dedupe_key": dedupe_key})
                continue
        inserted += 1
        ids.append(event["id"])
        results.append(
            {
                "status": "inserted",
                "event_id": event["id"],
                "dedupe_key": dedupe_key,
                "hash": event["hash"]["value"],
            }
        )
    conn.close()
    return {"new": inserted, "skipped": skipped, "failed": failed, "results": results, "errors": errors, "ids": ids}


def _validate_draft(draft: dict[str, Any]) -> str | None:
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


def _event_query_params(query: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "q": query.get("q", [None])[0],
        "types": _split_csv(query.get("type")),
        "tags": _split_csv(query.get("tag")),
        "after": query.get("after", [None])[0],
        "before": query.get("before", [None])[0],
        "limit": int(query.get("limit", ["50"])[0] or 50),
        "format": query.get("format", ["summary"])[0],
        "order": query.get("order", ["desc"])[0],
    }


def _split_csv(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    raw = ",".join(values)
    items = [item.strip() for item in raw.split(",") if item.strip()]
    return items or None


def _query_events(conn, config: OpsConfig, params: dict[str, Any]) -> list[dict[str, Any]]:
    q = params.get("q")
    types = params.get("types")
    tags = params.get("tags")
    after = params.get("after")
    before = params.get("before")
    limit = int(params.get("limit") or 50)
    format_mode = params.get("format") or "summary"
    order = params.get("order") or "desc"
    snippet_len = config.max_snippet_len

    conditions = []
    sql_params: list[Any] = []
    if types:
        placeholders = ",".join("?" for _ in types)
        conditions.append(f"e.type IN ({placeholders})")
        sql_params.extend(types)
    if tags:
        tag_conditions = []
        for tag in tags:
            tag_conditions.append("e.tags_json LIKE ?")
            sql_params.append(f'%"{tag}"%')
        conditions.append("(" + " OR ".join(tag_conditions) + ")")
    if after:
        conditions.append("e.ts >= ?")
        sql_params.append(after)
    if before:
        conditions.append("e.ts <= ?")
        sql_params.append(before)
    if q:
        if config.fts_enabled:
            conditions.append("events_fts MATCH ?")
            sql_params.append(q)
            table = "events_fts JOIN events e ON events_fts.rowid = e.rowid"
        else:
            conditions.append("e.text LIKE ?")
            sql_params.append(f"%{q}%")
            table = "events e"
    else:
        table = "events e"
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    order_clause = "ASC" if order == "asc" else "DESC"
    if format_mode == "full":
        query = (
            "SELECT e.* FROM "
            f"{table} "
            f"WHERE {where_clause} "
            f"ORDER BY e.ts {order_clause} "
            "LIMIT ?"
        )
        sql_params.append(limit)
        rows = conn.execute(query, sql_params).fetchall()
        return [_event_from_row(conn, row) for row in rows]

    query = (
        "SELECT e.id, e.ts, e.type, e.tags_json, substr(e.text, 1, ?) AS snippet "
        f"FROM {table} "
        f"WHERE {where_clause} "
        f"ORDER BY e.ts {order_clause} "
        "LIMIT ?"
    )
    rows = conn.execute(query, [snippet_len, *sql_params, limit]).fetchall()
    items = []
    for row in rows:
        refs = _fetch_refs(conn, row["id"])
        items.append(
            {
                "id": row["id"],
                "ts": row["ts"],
                "type": row["type"],
                "tags": json.loads(row["tags_json"]),
                "snippet": row["snippet"],
                "refs": refs,
            }
        )
    return items


def _fetch_event(conn, event_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
    if not row:
        return None
    return _event_from_row(conn, row)


def _fetch_refs(conn, event_id: str) -> list[dict[str, Any]]:
    refs_rows = conn.execute("SELECT * FROM refs WHERE event_id = ? ORDER BY id", (event_id,)).fetchall()
    refs: list[dict[str, Any]] = []
    for ref in refs_rows:
        digest = None
        if ref["digest_algo"]:
            digest = {"algo": ref["digest_algo"], "value": ref["digest_value"]}
        refs.append(
            {
                "kind": ref["ref_kind"],
                "uri": ref["uri"],
                "span": json.loads(ref["span_json"]),
                "digest": digest,
            }
        )
    return refs


def _event_from_row(conn, row) -> dict[str, Any]:
    refs = _fetch_refs(conn, row["id"])
    event = {
        "schema_version": row["schema_version"],
        "id": row["id"],
        "ts": row["ts"],
        "type": row["type"],
        "source": {
            "kind": row["source_kind"],
            "locator": row["source_locator"],
            "meta": json.loads(row["source_meta_json"]),
        },
        "refs": refs,
        "tags": json.loads(row["tags_json"]),
        "text": row["text"],
        "payload": json.loads(row["payload_json"]),
        "hash": {"algo": row["hash_algo"], "value": row["hash_value"]},
        "dedupe_key": row["dedupe_key"],
    }
    return event


def _merge_view_filters(view_query: dict[str, Any], request_filters: dict[str, Any]) -> dict[str, Any]:
    query = dict(view_query)
    stored_filters = query.get("filters", {}) if isinstance(query.get("filters", {}), dict) else {}
    merged: dict[str, Any] = dict(stored_filters)
    request_filters = request_filters or {}

    stored_types = stored_filters.get("type")
    request_types = request_filters.get("type")
    if stored_types and request_types:
        merged["type"] = [value for value in stored_types if value in request_types]
    elif request_types is not None:
        merged["type"] = request_types

    stored_tags = stored_filters.get("tag")
    request_tags = request_filters.get("tag")
    if stored_tags and request_tags:
        merged["tag"] = _merge_tags(stored_tags, request_tags)
    elif request_tags is not None:
        merged["tag"] = request_tags

    stored_after = stored_filters.get("after")
    request_after = request_filters.get("after")
    if stored_after and request_after:
        merged["after"] = max(stored_after, request_after)
    elif request_after:
        merged["after"] = request_after

    stored_before = stored_filters.get("before")
    request_before = request_filters.get("before")
    if stored_before and request_before:
        merged["before"] = min(stored_before, request_before)
    elif request_before:
        merged["before"] = request_before

    query["filters"] = merged
    return query


def _artifact_from_event(event: dict[str, Any]) -> dict[str, Any]:
    path = None
    for ref in event.get("refs", []):
        uri = ref.get("uri")
        if isinstance(uri, str) and uri.startswith("file:"):
            path = uri[5:]
            break
    kind = "other"
    if path:
        ext = Path(path).suffix.lower()
        if ext == ".md":
            kind = "markdown"
        elif ext == ".json":
            kind = "json"
    return {
        "path": path,
        "kind": kind,
        "created_at": event["ts"],
        "refs": event.get("refs", []),
        "event_id": event["id"],
    }


def _execute_job(server: OpsdServer, job: dict[str, Any]) -> dict[str, Any]:
    kind = job.get("kind")
    if kind == "daily_digest":
        return _run_daily_digest(server, job)
    if kind == "artifact_pack":
        tag = job.get("config", {}).get("tag")
        out_dir = job.get("config", {}).get("out_dir")
        if not tag or not out_dir:
            raise ValueError("artifact_pack config requires tag and out_dir")
        return _run_artifact_pack(server, tag=tag, out_dir=out_dir)
    raise ValueError(f"Unsupported job kind: {kind}")


def _run_daily_digest(server: OpsdServer, job: dict[str, Any]) -> dict[str, Any]:
    config = job.get("config", {})
    view_name = config.get("view")
    day_value = config.get("day")
    out_dir = config.get("out_dir")
    if not view_name or not day_value or not out_dir:
        raise ValueError("daily_digest requires view, day, and out_dir")
    tz = ZoneInfo(server.config.timezone)
    day = date.fromisoformat(day_value)
    start_dt = datetime.combine(day, datetime.min.time()).replace(tzinfo=tz)
    end_dt = start_dt + timedelta(days=1)
    conn = connect(server.paths["db"])
    view_row = conn.execute("SELECT * FROM views WHERE name = ?", (view_name,)).fetchone()
    if not view_row:
        conn.close()
        raise ValueError(f"View not found: {view_name}")
    view = _view_from_row(view_row)
    merged_query = _merge_view_filters(view.get("query", {}), {"after": start_dt.isoformat(), "before": end_dt.isoformat()})
    params = {
        "q": None,
        "types": merged_query.get("filters", {}).get("type"),
        "tags": merged_query.get("filters", {}).get("tag"),
        "after": merged_query.get("filters", {}).get("after"),
        "before": merged_query.get("filters", {}).get("before"),
        "limit": 500,
        "format": "summary",
        "order": merged_query.get("order", "desc"),
    }
    items = _query_events(conn, server.config, params)
    conn.close()

    type_counts = Counter(item["type"] for item in items)
    tag_counts = Counter(tag for item in items for tag in item.get("tags", []))
    snippets = [item.get("snippet") for item in items if item.get("snippet")]
    markdown_lines = [
        f"# Daily Digest {day_value}",
        "",
        "## Counts by type",
    ]
    for typ, count in type_counts.most_common():
        markdown_lines.append(f"- {typ}: {count}")
    markdown_lines.append("")
    markdown_lines.append("## Top tags")
    for tag, count in tag_counts.most_common(10):
        markdown_lines.append(f"- {tag}: {count}")
    markdown_lines.append("")
    markdown_lines.append("## Sample snippets")
    for snippet in snippets[:10]:
        markdown_lines.append(f"- {snippet}")
    markdown = "\n".join(markdown_lines) + "\n"

    output_dir = server.paths["workspace"] / out_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    digest_path = output_dir / "daily_digest.md"
    digest_path.write_text(markdown, encoding="utf-8")

    tags = ["digest"]
    extra_tags = config.get("tags")
    if isinstance(extra_tags, list):
        tags = _merge_tags(tags, extra_tags)
    _emit_artifact_event(
        server,
        refs=[{"kind": "file", "uri": f"file:{digest_path}"}],
        tags=tags,
        payload={"path": str(digest_path), "job": job.get("name")},
    )
    return {"artifact_paths": [str(digest_path)]}


def _run_artifact_pack(server: OpsdServer, tag: str, out_dir: str) -> dict[str, Any]:
    conn = connect(server.paths["db"])
    params = {
        "q": None,
        "types": None,
        "tags": [tag],
        "after": None,
        "before": None,
        "limit": 500,
        "format": "full",
        "order": "desc",
    }
    events = _query_events(conn, server.config, params)
    conn.close()

    output_dir = server.paths["workspace"] / out_dir
    assets_dir = output_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    copied_assets = []
    for event in events:
        if event.get("type") != "artifact.created":
            continue
        for ref in event.get("refs", []):
            uri = ref.get("uri")
            if not isinstance(uri, str) or not uri.startswith("file:"):
                continue
            source_path = Path(uri[5:])
            if not source_path.exists():
                continue
            digest = sha256_hex(source_path.read_bytes())
            dest = assets_dir / f"{digest[:12]}_{source_path.name}"
            try:
                shutil.copy2(source_path, dest)
            except OSError:
                continue
            copied_assets.append(str(dest))

    pack = {"tag": tag, "items": events, "assets": copied_assets}
    output_dir.mkdir(parents=True, exist_ok=True)
    pack_path = output_dir / "pack.json"
    pack_path.write_text(json.dumps(pack, ensure_ascii=False, indent=2), encoding="utf-8")

    readme_lines = [f"# Artifact Pack {tag}", "", f"Total items: {len(events)}", ""]
    for item in events[:20]:
        readme_lines.append(f"- {item['id']} {item['type']} {item['ts']}")
    readme_path = output_dir / "README.md"
    readme_path.write_text("\n".join(readme_lines) + "\n", encoding="utf-8")

    _emit_artifact_event(
        server,
        refs=[
            {"kind": "file", "uri": f"file:{pack_path}"},
            {"kind": "file", "uri": f"file:{readme_path}"},
        ],
        tags=[tag, "artifact-pack"],
        payload={"paths": [str(pack_path), str(readme_path)], "tag": tag},
    )

    return {"pack_path": str(pack_path), "readme_path": str(readme_path), "assets": copied_assets}


def _emit_artifact_event(server: OpsdServer, refs: list[dict[str, Any]], tags: list[str], payload: dict[str, Any]) -> None:
    event_core = {
        "schema_version": "0.2",
        "ts": iso_now(server.config.timezone),
        "type": "artifact.created",
        "source": {"kind": "job", "locator": "opsd", "meta": {}},
        "refs": refs,
        "tags": tags,
        "text": "artifact created",
        "payload": payload,
    }
    event = {
        **event_core,
        "id": generate_ulid(),
        "hash": event_hash(event_core),
        "dedupe_key": dedupe_key_from_event(event_core),
    }
    append_event(server.paths["events"], event)
    conn = connect(server.paths["db"])
    created_at = iso_now(server.config.timezone)
    with conn:
        _insert_event(conn, event, event.get("dedupe_key"), created_at)
    conn.close()


def _ensure_builtin_views(conn, timezone: str) -> None:
    created_at = iso_now(timezone)
    for name, query in [
        ("timeline", {"kind": "events_query", "filters": {}, "order": "desc"}),
        ("tag_timeline", {"kind": "events_query", "filters": {}, "order": "desc"}),
    ]:
        row = conn.execute("SELECT 1 FROM views WHERE name = ?", (name,)).fetchone()
        if row:
            continue
        conn.execute(
            "INSERT INTO views (name, description, query_json, created_at) VALUES (?, ?, ?, ?)",
            (name, "", json.dumps(query, ensure_ascii=False), created_at),
        )


def run_opsd(host: str | None = None, port: int | None = None) -> None:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    _ensure_workspace(paths)
    init_db(paths["db"])
    conn = connect(paths["db"])
    with conn:
        _ensure_builtin_views(conn, config.timezone)
    conn.close()

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
