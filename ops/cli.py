from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import adapters
from .canonical import append_event
from .client import OpsdClient, OpsdClientError
from .config import OpsConfig, load_config, write_default_config
from .db import connect, init_db
from .errors import AdapterError, DatabaseError, OpsError
from .events import dedupe_key_from_draft, event_hash
from .lock import FileLock
from .utils import generate_ulid, iso_from_timestamp, iso_now, sha256_hex

DEFAULT_ENDPOINT = "http://127.0.0.1:7777"


@dataclass
class IngestResult:
    new: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list[str] | None = None


def _workspace_paths(config: OpsConfig) -> dict[str, Path]:
    workspace = config.workspace
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


def _print_output(payload: Any, json_flag: bool) -> None:
    if json_flag:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        if isinstance(payload, str):
            print(payload)
        else:
            print(json.dumps(payload, ensure_ascii=False, indent=2))


def _client(endpoint: str) -> OpsdClient:
    return OpsdClient(endpoint=endpoint, timeout=1.0)


def _check_online(endpoint: str) -> bool:
    client = _client(endpoint)
    try:
        client.health()
    except OpsdClientError:
        return False
    return True


def cmd_init(args: argparse.Namespace) -> int:
    root = Path.cwd()
    ops_yml = root / "ops.yml"
    if not ops_yml.exists():
        write_default_config(ops_yml)
    config = load_config(ops_yml)
    paths = _workspace_paths(config)
    _ensure_workspace(paths)
    init_db(paths["db"])
    conn = connect(paths["db"])
    with conn:
        _ensure_builtin_views(conn, config.timezone)
    conn.close()
    print(f"Initialized workspace at {paths['workspace']}")
    print("canonical/events.jsonl OK")
    print("index/brain.sqlite OK")
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    from .daemon import run_opsd

    run_opsd(host=args.host, port=args.port)
    return 0


def cmd_source_add(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        raise OpsError("opsd is not reachable")
    payload = {
        "name": args.name,
        "kind": "chat_json_file",
        "config": {"path": args.path, "copy": not args.no_copy},
        "tags": args.tags or [],
    }
    response = _client(args.endpoint).create_source(payload)
    _print_output(response, args.json)
    return 0


def cmd_source_list(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        if args.offline:
            return _local_source_list(args)
        raise OpsError("opsd is not reachable")
    response = _client(args.endpoint).list_sources()
    _print_output(response if args.json else response.get("items", []), args.json)
    return 0


def cmd_source_show(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        if args.offline:
            return _local_source_show(args)
        raise OpsError("opsd is not reachable")
    response = _client(args.endpoint).get_source(args.name)
    _print_output(response, args.json)
    return 0


def cmd_source_rm(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        raise OpsError("opsd is not reachable")
    response = _client(args.endpoint).delete_source(args.name)
    _print_output(response, args.json)
    return 0


def cmd_source_test(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        raise OpsError("opsd is not reachable")
    response = _client(args.endpoint).test_source(args.name)
    _print_output(response, args.json)
    return 0


def cmd_ingest_run(args: argparse.Namespace) -> int:
    if _check_online(args.endpoint) and not args.offline:
        payload = {"tags": args.tags or [], "dry_run": args.dry_run}
        response = _client(args.endpoint).run_ingest(args.name, payload)
        _print_output(response, args.json)
        return 0
    if not args.offline:
        raise OpsError("opsd is not reachable (use --offline to ingest locally)")
    response = _local_ingest_run(args)
    _print_output(response, args.json)
    return 0


def cmd_ingest_chat_json(args: argparse.Namespace) -> int:
    if _check_online(args.endpoint) and not args.offline:
        raise OpsError("Use ops ingest run for HTTP ingest (or pass --offline)")
    if not args.offline:
        raise OpsError("opsd is not reachable (use --offline to ingest locally)")
    response = _local_ingest_chat_json(args)
    _print_output(response, args.json)
    return 0


def cmd_view_add(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        raise OpsError("opsd is not reachable")
    query = json.loads(args.query)
    payload = {"name": args.name, "description": args.description or "", "query": query}
    response = _client(args.endpoint).create_view(payload)
    _print_output(response, args.json)
    return 0


def cmd_view_list(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        if args.offline:
            return _local_view_list(args)
        raise OpsError("opsd is not reachable")
    response = _client(args.endpoint).list_views()
    _print_output(response if args.json else response.get("items", []), args.json)
    return 0


def cmd_view_show(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        if args.offline:
            return _local_view_show(args)
        raise OpsError("opsd is not reachable")
    response = _client(args.endpoint).get_view(args.name)
    _print_output(response, args.json)
    return 0


def cmd_view_rm(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        raise OpsError("opsd is not reachable")
    response = _client(args.endpoint).delete_view(args.name)
    _print_output(response, args.json)
    return 0


def cmd_view_query(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        if args.offline:
            return _local_view_query(args)
        raise OpsError("opsd is not reachable")
    payload = {"filters": json.loads(args.filters) if args.filters else {}, "limit": args.limit}
    response = _client(args.endpoint).query_view(args.name, payload)
    _print_output(response if args.json else response.get("items", []), args.json)
    return 0


def cmd_job_add(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        raise OpsError("opsd is not reachable")
    payload = {
        "name": args.name,
        "kind": args.kind,
        "config": _parse_config_args(args.config),
        "enabled": not args.disabled,
    }
    response = _client(args.endpoint).create_job(payload)
    _print_output(response, args.json)
    return 0


def cmd_job_list(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        if args.offline:
            return _local_job_list(args)
        raise OpsError("opsd is not reachable")
    response = _client(args.endpoint).list_jobs()
    _print_output(response if args.json else response.get("items", []), args.json)
    return 0


def cmd_job_show(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        if args.offline:
            return _local_job_show(args)
        raise OpsError("opsd is not reachable")
    response = _client(args.endpoint).get_job(args.name)
    _print_output(response, args.json)
    return 0


def cmd_job_rm(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        raise OpsError("opsd is not reachable")
    response = _client(args.endpoint).delete_job(args.name)
    _print_output(response, args.json)
    return 0


def cmd_job_run(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        raise OpsError("opsd is not reachable")
    response = _client(args.endpoint).run_job(args.name, {})
    _print_output(response, args.json)
    return 0


def cmd_job_logs(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        if args.offline:
            return _local_job_logs(args)
        raise OpsError("opsd is not reachable")
    response = _client(args.endpoint).job_runs(args.name)
    _print_output(response if args.json else response.get("items", []), args.json)
    return 0


def cmd_artifact_list(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        if args.offline:
            return _local_artifact_list(args)
        raise OpsError("opsd is not reachable")
    params = {"tag": args.tag, "after": args.after, "before": args.before}
    response = _client(args.endpoint).list_artifacts(params)
    _print_output(response if args.json else response.get("items", []), args.json)
    return 0


def cmd_artifact_pack(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        raise OpsError("opsd is not reachable")
    payload = {"tag": args.tag, "out_dir": args.out_dir}
    response = _client(args.endpoint).pack_artifacts(payload)
    _print_output(response, args.json)
    return 0


def cmd_artifact_open(args: argparse.Namespace) -> int:
    path = Path(args.path)
    if not path.exists():
        raise OpsError(f"Artifact not found: {args.path}")
    if sys.platform.startswith("darwin"):
        subprocess.run(["open", str(path)], check=False)
    elif os.name == "nt":
        subprocess.run(["start", str(path)], shell=True, check=False)
    else:
        subprocess.run(["xdg-open", str(path)], check=False)
    return 0


def cmd_search(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        if args.offline:
            return _local_search(args)
        raise OpsError("opsd is not reachable")
    params = {
        "q": args.query,
        "type": ",".join(args.types) if args.types else None,
        "tag": ",".join(args.tags) if args.tags else None,
        "after": args.after,
        "before": args.before,
        "limit": args.limit,
        "format": args.format,
    }
    response = _client(args.endpoint).get_events(params)
    _print_output(response if args.json else response.get("items", []), args.json)
    return 0


def cmd_event_show(args: argparse.Namespace) -> int:
    if not _check_online(args.endpoint):
        if args.offline:
            return _local_event_show(args)
        raise OpsError("opsd is not reachable")
    response = _client(args.endpoint).get_event(args.event_id)
    _print_output(response, args.json)
    return 0


def _local_source_list(args: argparse.Namespace) -> int:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    rows = conn.execute("SELECT * FROM sources ORDER BY created_at DESC").fetchall()
    conn.close()
    items = [_row_to_source(row) for row in rows]
    _print_output(items if not args.json else {"items": items}, args.json)
    return 0


def _local_source_show(args: argparse.Namespace) -> int:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    row = conn.execute("SELECT * FROM sources WHERE name = ?", (args.name,)).fetchone()
    conn.close()
    if not row:
        raise OpsError("Source not found")
    _print_output(_row_to_source(row), args.json)
    return 0


def _local_view_list(args: argparse.Namespace) -> int:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    rows = conn.execute("SELECT * FROM views ORDER BY created_at DESC").fetchall()
    conn.close()
    items = [_row_to_view(row) for row in rows]
    _print_output(items if not args.json else {"items": items}, args.json)
    return 0


def _local_view_show(args: argparse.Namespace) -> int:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    row = conn.execute("SELECT * FROM views WHERE name = ?", (args.name,)).fetchone()
    conn.close()
    if not row:
        raise OpsError("View not found")
    _print_output(_row_to_view(row), args.json)
    return 0


def _local_view_query(args: argparse.Namespace) -> int:
    from .daemon import _merge_view_filters, _query_events

    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    row = conn.execute("SELECT * FROM views WHERE name = ?", (args.name,)).fetchone()
    if not row:
        conn.close()
        raise OpsError("View not found")
    view = _row_to_view(row)
    filters = json.loads(args.filters) if args.filters else {}
    merged = _merge_view_filters(view.get("query", {}), filters)
    params = {
        "q": None,
        "types": merged.get("filters", {}).get("type"),
        "tags": merged.get("filters", {}).get("tag"),
        "after": merged.get("filters", {}).get("after"),
        "before": merged.get("filters", {}).get("before"),
        "limit": args.limit,
        "format": "summary",
        "order": merged.get("order", "desc"),
    }
    items = _query_events(conn, config, params)
    conn.close()
    _print_output(items if not args.json else {"items": items}, args.json)
    return 0


def _local_job_list(args: argparse.Namespace) -> int:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    rows = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()
    conn.close()
    items = [_row_to_job(row) for row in rows]
    _print_output(items if not args.json else {"items": items}, args.json)
    return 0


def _local_job_show(args: argparse.Namespace) -> int:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    row = conn.execute("SELECT * FROM jobs WHERE name = ?", (args.name,)).fetchone()
    conn.close()
    if not row:
        raise OpsError("Job not found")
    _print_output(_row_to_job(row), args.json)
    return 0


def _local_job_logs(args: argparse.Namespace) -> int:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    rows = conn.execute(
        "SELECT * FROM job_runs WHERE job_name = ? ORDER BY started_at DESC",
        (args.name,),
    ).fetchall()
    conn.close()
    items = [_row_to_job_run(row) for row in rows]
    _print_output(items if not args.json else {"items": items}, args.json)
    return 0


def _local_artifact_list(args: argparse.Namespace) -> int:
    from .daemon import _artifact_from_event, _query_events

    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    params = {
        "q": None,
        "types": ["artifact.created"],
        "tags": [args.tag] if args.tag else None,
        "after": args.after,
        "before": args.before,
        "limit": 200,
        "format": "full",
        "order": "desc",
    }
    events = _query_events(conn, config, params)
    conn.close()
    items = [_artifact_from_event(event) for event in events]
    _print_output(items if not args.json else {"items": items}, args.json)
    return 0


def _local_search(args: argparse.Namespace) -> int:
    from .daemon import _query_events

    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    params = {
        "q": args.query,
        "types": args.types,
        "tags": args.tags,
        "after": args.after,
        "before": args.before,
        "limit": args.limit,
        "format": args.format,
        "order": "desc",
    }
    items = _query_events(conn, config, params)
    conn.close()
    _print_output(items if not args.json else {"items": items}, args.json)
    return 0


def _local_event_show(args: argparse.Namespace) -> int:
    from .daemon import _fetch_event

    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    event = _fetch_event(conn, args.event_id)
    conn.close()
    if not event:
        raise OpsError("Event not found")
    _print_output(event, args.json)
    return 0


def _row_to_source(row) -> dict[str, Any]:
    return {
        "name": row["name"],
        "kind": row["kind"],
        "config": json.loads(row["config_json"]),
        "tags": json.loads(row["tags_json"]),
        "created_at": row["created_at"],
    }


def _row_to_view(row) -> dict[str, Any]:
    return {
        "name": row["name"],
        "description": row["description"],
        "query": json.loads(row["query_json"]),
        "created_at": row["created_at"],
    }


def _row_to_job(row) -> dict[str, Any]:
    return {
        "name": row["name"],
        "kind": row["kind"],
        "config": json.loads(row["config_json"]),
        "enabled": bool(row["enabled"]),
        "created_at": row["created_at"],
    }


def _row_to_job_run(row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "job_name": row["job_name"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "status": row["status"],
        "outputs": json.loads(row["output_json"]),
        "error": row["error"],
    }


def _local_ingest_run(args: argparse.Namespace) -> dict[str, Any]:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    row = conn.execute("SELECT * FROM sources WHERE name = ?", (args.name,)).fetchone()
    conn.close()
    if not row:
        raise OpsError("Source not found")
    source = _row_to_source(row)
    drafts = _build_source_drafts(source, args.tags or [], config)
    result = IngestResult(errors=[])
    _local_ingest_with_lock(paths, drafts, result, config.timezone, args.dry_run)
    return {"new": result.new, "skipped": result.skipped, "failed": result.failed, "errors": result.errors or []}


def _local_ingest_chat_json(args: argparse.Namespace) -> dict[str, Any]:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    drafts = _build_chat_drafts(Path(args.path), args.tags or [], config, args.copy)
    result = IngestResult(errors=[])
    _local_ingest_with_lock(paths, drafts, result, config.timezone, False)
    return {"new": result.new, "skipped": result.skipped, "failed": result.failed, "errors": result.errors or []}


def _build_chat_drafts(
    source_path: Path,
    tags: list[str],
    config: OpsConfig,
    copy: bool,
) -> list[dict[str, Any]]:
    source_path = source_path if source_path.is_absolute() else (Path.cwd() / source_path)
    locator_path = source_path
    locator_value = str(locator_path)
    if copy:
        locator_path = _copy_source(source_path, config.workspace / "raw" / "chat_json")
        locator_value = str(locator_path)
    drafts = []
    for idx, message in enumerate(adapters.iter_chat_messages(locator_path)):
        content = message.get("content")
        if content is None:
            raise OpsError(f"Missing content at idx {idx}")
        ts_value = message.get("ts")
        if not ts_value:
            ts_value = iso_from_timestamp(locator_path.stat().st_mtime, config.timezone)
        text = content.replace("\r\n", "\n").replace("\r", "\n")
        payload = {"speaker": message.get("speaker"), "content": content}
        if message.get("thread_id") is not None:
            payload["thread_id"] = message.get("thread_id")
        draft = {
            "schema_version": "0.2",
            "ts": ts_value,
            "type": "chat.message",
            "source": {"kind": "chat_json_file", "locator": locator_value, "meta": {}},
            "refs": [{"kind": "file", "uri": f"file:{locator_value}", "span": {"idx": idx}}],
            "tags": tags,
            "text": text,
            "payload": payload,
        }
        drafts.append(draft)
    return drafts


def _build_source_drafts(source: dict[str, Any], extra_tags: list[str], config: OpsConfig) -> list[dict[str, Any]]:
    cfg = dict(source.get("config", {}))
    if "copy" not in cfg:
        cfg["copy"] = True
    path_value = cfg.get("path")
    if not isinstance(path_value, str) or not path_value:
        raise OpsError("config.path is required")
    source_path = Path(path_value)
    if not source_path.is_absolute():
        source_path = Path.cwd() / source_path
    locator_path = source_path
    locator_value = str(locator_path)
    if cfg.get("copy", True):
        locator_path = _copy_source(source_path, config.workspace / "raw" / "chat_json")
        locator_value = str(locator_path)
    tags = _merge_tags(source.get("tags", []), extra_tags)
    drafts = []
    for idx, message in enumerate(adapters.iter_chat_messages(locator_path)):
        content = message.get("content")
        if content is None:
            raise OpsError(f"Missing content at idx {idx}")
        ts_value = message.get("ts")
        if not ts_value:
            ts_value = iso_from_timestamp(locator_path.stat().st_mtime, config.timezone)
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


def _parse_config_args(values: list[str] | None) -> dict[str, Any]:
    if not values:
        return {}
    if len(values) == 1 and values[0].lstrip().startswith("{"):
        return json.loads(values[0])
    config: dict[str, Any] = {}
    for item in values:
        if "=" not in item:
            raise OpsError(f"Invalid config entry: {item}")
        key, raw_value = item.split("=", 1)
        value = raw_value.strip()
        if value.lower() in {"true", "false"}:
            parsed: Any = value.lower() == "true"
        elif value.isdigit():
            parsed = int(value)
        else:
            parsed = value
        config[key.strip()] = parsed
    return config


def _copy_source(path: Path, dest_dir: Path) -> Path:
    digest = sha256_hex(path.read_bytes())
    dest = dest_dir / f"{digest[:12]}_{path.name}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(path.read_bytes())
    return dest


def _local_ingest_with_lock(
    paths: dict[str, Path],
    drafts: list[dict[str, Any]],
    result: IngestResult,
    timezone: str,
    dry_run: bool,
) -> None:
    lock_path = paths["canonical"] / ".ops.lock"
    timeout = float(os.environ.get("OPS_LOCK_TIMEOUT", "10"))
    with FileLock(lock_path, timeout=timeout):
        conn = connect(paths["db"])
        created_at = iso_now(timezone)
        for draft in drafts:
            dedupe = dedupe_key_from_draft(draft)
            if not dedupe:
                result.failed += 1
                result.errors.append("Unable to compute dedupe_key")
                continue
            row = conn.execute(
                "SELECT event_id FROM dedupe WHERE dedupe_key = ?",
                (dedupe,),
            ).fetchone()
            if row:
                result.skipped += 1
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
                "dedupe_key": dedupe,
            }
            if not dry_run:
                append_event(paths["events"], event)
                try:
                    with conn:
                        _insert_event(conn, event, dedupe, created_at)
                except DatabaseError:
                    result.failed += 1
                    result.errors.append("SQLite insert failed")
                    continue
            result.new += 1
        conn.close()


def _insert_event(conn, event: dict[str, Any], dedupe_key: str | None, created_at: str) -> None:
    try:
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
                json.dumps(event["tags"], ensure_ascii=False),
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
    except Exception as exc:  # noqa: BLE001
        raise DatabaseError(f"SQLite insert error: {exc}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ops")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("init")

    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=7777)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    common.add_argument("--json", action="store_true")
    common.add_argument("--offline", action="store_true")

    source_parser = subparsers.add_parser("source")
    source_sub = source_parser.add_subparsers(dest="action")
    source_add = source_sub.add_parser("add", parents=[common])
    source_add.add_argument("name")
    source_add.add_argument("--path", required=True)
    source_add.add_argument("--tag", dest="tags", action="append")
    source_add.add_argument("--no-copy", action="store_true")
    source_list = source_sub.add_parser("list", parents=[common])
    source_show = source_sub.add_parser("show", parents=[common])
    source_show.add_argument("name")
    source_rm = source_sub.add_parser("rm", parents=[common])
    source_rm.add_argument("name")
    source_test = source_sub.add_parser("test", parents=[common])
    source_test.add_argument("name")

    ingest_parser = subparsers.add_parser("ingest")
    ingest_sub = ingest_parser.add_subparsers(dest="action")
    ingest_run = ingest_sub.add_parser("run", parents=[common])
    ingest_run.add_argument("name")
    ingest_run.add_argument("--tag", dest="tags", action="append")
    ingest_run.add_argument("--dry-run", action="store_true")
    ingest_chat = ingest_sub.add_parser("chat_json", parents=[common])
    ingest_chat.add_argument("path")
    ingest_chat.add_argument("--tag", dest="tags", action="append")
    ingest_chat.add_argument("--copy", dest="copy", action="store_true")
    ingest_chat.add_argument("--no-copy", dest="copy", action="store_false")
    ingest_chat.set_defaults(copy=True)

    view_parser = subparsers.add_parser("view")
    view_sub = view_parser.add_subparsers(dest="action")
    view_add = view_sub.add_parser("add", parents=[common])
    view_add.add_argument("name")
    view_add.add_argument("--description")
    view_add.add_argument("--query", required=True)
    view_list = view_sub.add_parser("list", parents=[common])
    view_show = view_sub.add_parser("show", parents=[common])
    view_show.add_argument("name")
    view_rm = view_sub.add_parser("rm", parents=[common])
    view_rm.add_argument("name")
    view_query = view_sub.add_parser("query", parents=[common])
    view_query.add_argument("name")
    view_query.add_argument("--filters")
    view_query.add_argument("--limit", type=int, default=50)

    job_parser = subparsers.add_parser("job")
    job_sub = job_parser.add_subparsers(dest="action")
    job_add = job_sub.add_parser("add", parents=[common])
    job_add.add_argument("name")
    job_add.add_argument("--kind", required=True)
    job_add.add_argument("--config", action="append")
    job_add.add_argument("--disabled", action="store_true")
    job_list = job_sub.add_parser("list", parents=[common])
    job_show = job_sub.add_parser("show", parents=[common])
    job_show.add_argument("name")
    job_rm = job_sub.add_parser("rm", parents=[common])
    job_rm.add_argument("name")
    job_run = job_sub.add_parser("run", parents=[common])
    job_run.add_argument("name")
    job_logs = job_sub.add_parser("logs", parents=[common])
    job_logs.add_argument("name")

    artifact_parser = subparsers.add_parser("artifact")
    artifact_sub = artifact_parser.add_subparsers(dest="action")
    artifact_list = artifact_sub.add_parser("list", parents=[common])
    artifact_list.add_argument("--tag")
    artifact_list.add_argument("--after")
    artifact_list.add_argument("--before")
    artifact_pack = artifact_sub.add_parser("pack", parents=[common])
    artifact_pack.add_argument("--tag", required=True)
    artifact_pack.add_argument("--out-dir", required=True)
    artifact_open = artifact_sub.add_parser("open")
    artifact_open.add_argument("path")

    search_parser = subparsers.add_parser("search", parents=[common])
    search_parser.add_argument("query")
    search_parser.add_argument("--type", dest="types", action="append")
    search_parser.add_argument("--tag", dest="tags", action="append")
    search_parser.add_argument("--after")
    search_parser.add_argument("--before")
    search_parser.add_argument("--limit", type=int, default=50)
    search_parser.add_argument("--format", choices=["summary", "full"], default="summary")

    event_parser = subparsers.add_parser("event", parents=[common])
    event_sub = event_parser.add_subparsers(dest="action")
    event_show = event_sub.add_parser("show", parents=[common])
    event_show.add_argument("event_id")

    return parser


def run(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help(sys.stderr)
        return 2
    if args.command == "init":
        return cmd_init(args)
    if args.command == "serve":
        return cmd_serve(args)
    if args.command == "source":
        if args.action == "add":
            return cmd_source_add(args)
        if args.action == "list":
            return cmd_source_list(args)
        if args.action == "show":
            return cmd_source_show(args)
        if args.action == "rm":
            return cmd_source_rm(args)
        if args.action == "test":
            return cmd_source_test(args)
    if args.command == "ingest":
        if args.action == "run":
            return cmd_ingest_run(args)
        if args.action == "chat_json":
            return cmd_ingest_chat_json(args)
    if args.command == "view":
        if args.action == "add":
            return cmd_view_add(args)
        if args.action == "list":
            return cmd_view_list(args)
        if args.action == "show":
            return cmd_view_show(args)
        if args.action == "rm":
            return cmd_view_rm(args)
        if args.action == "query":
            return cmd_view_query(args)
    if args.command == "job":
        if args.action == "add":
            return cmd_job_add(args)
        if args.action == "list":
            return cmd_job_list(args)
        if args.action == "show":
            return cmd_job_show(args)
        if args.action == "rm":
            return cmd_job_rm(args)
        if args.action == "run":
            return cmd_job_run(args)
        if args.action == "logs":
            return cmd_job_logs(args)
    if args.command == "artifact":
        if args.action == "list":
            return cmd_artifact_list(args)
        if args.action == "pack":
            return cmd_artifact_pack(args)
        if args.action == "open":
            return cmd_artifact_open(args)
    if args.command == "search":
        return cmd_search(args)
    if args.command == "event":
        if args.action == "show":
            return cmd_event_show(args)
    raise AdapterError("Unknown command")


def main() -> None:
    try:
        code = run()
        raise SystemExit(code)
    except OpsError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(getattr(exc, "exit_code", 1)) from exc
    except argparse.ArgumentError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(50) from exc
