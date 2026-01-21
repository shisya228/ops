from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import adapters
from .canonical import append_event
from .config import OpsConfig, load_config, write_default_config
from .db import connect, init_db
from .errors import AdapterError, ConfigError, DatabaseError, IOError, OpsError
from .utils import generate_ulid, iso_from_timestamp, iso_now, normalize_text, sha256_hex


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
        "artifacts": workspace / "artifacts" / "runs",
    }


def _event_hash(event_data: dict[str, Any]) -> dict[str, str]:
    payload = json.dumps(
        event_data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {"algo": "sha256", "value": sha256_hex(payload.encode("utf-8"))}


def _dedupe_key(adapter: str, locator: str, idx: int, content: str) -> str:
    span_key = f"idx:{idx}"
    norm_text = normalize_text(content)
    material = f"{adapter}|{locator}|{span_key}|{norm_text}"
    return sha256_hex(material.encode("utf-8"))


def cmd_init(args: argparse.Namespace) -> int:
    root = Path.cwd()
    ops_yml = root / "ops.yml"
    if not ops_yml.exists():
        write_default_config(ops_yml)
    config = load_config(ops_yml)
    paths = _workspace_paths(config)
    paths["raw"].mkdir(parents=True, exist_ok=True)
    paths["canonical"].mkdir(parents=True, exist_ok=True)
    paths["index"].mkdir(parents=True, exist_ok=True)
    paths["artifacts"].mkdir(parents=True, exist_ok=True)
    if not paths["events"].exists():
        paths["events"].write_text("", encoding="utf-8")
    init_db(paths["db"])
    print(f"Initialized workspace at {paths['workspace']}")
    print("canonical/events.jsonl OK")
    print("index/brain.sqlite OK")
    return 0


def _copy_source(path: Path, dest_dir: Path) -> Path:
    digest = sha256_hex(path.read_bytes())
    dest = dest_dir / f"{digest[:12]}_{path.name}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, dest)
    return dest


def _insert_event(
    conn: sqlite3.Connection,
    event: dict[str, Any],
    dedupe_key: str | None,
    created_at: str,
) -> None:
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
    except sqlite3.Error as exc:
        raise DatabaseError(f"SQLite insert error: {exc}") from exc


def cmd_ingest(args: argparse.Namespace) -> int:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    source_path = Path(args.path)
    if args.copy:
        locator_path = _copy_source(source_path, paths["raw"])
    else:
        locator_path = source_path
    locator = str(locator_path)
    result = IngestResult(errors=[])
    adapter_name = "chat_json"
    try:
        conn = connect(paths["db"])
    except DatabaseError:
        raise
    created_at = iso_now(config.timezone)
    for idx, message in enumerate(adapters.iter_chat_messages(source_path)):
        content = message.get("content")
        if content is None:
            result.failed += 1
            result.errors.append(f"Missing content at idx {idx}")
            continue
        dedupe_key = _dedupe_key(adapter_name, locator, idx, content)
        row = conn.execute(
            "SELECT event_id FROM dedupe WHERE dedupe_key = ?",
            (dedupe_key,),
        ).fetchone()
        if row:
            result.skipped += 1
            continue
        ts_value = message.get("ts")
        if not ts_value:
            ts_value = iso_from_timestamp(source_path.stat().st_mtime, config.timezone)
        text = content.replace("\r\n", "\n").replace("\r", "\n")
        payload = {
            "speaker": message.get("speaker"),
            "content": content,
        }
        if message.get("thread_id") is not None:
            payload["thread_id"] = message.get("thread_id")
        event_core = {
            "schema_version": "0.1",
            "ts": ts_value,
            "type": "chat.message",
            "source": {
                "kind": adapter_name,
                "locator": locator,
                "meta": {},
            },
            "refs": [
                {
                    "kind": "file",
                    "uri": f"file:{locator}",
                    "span": {"idx": idx},
                }
            ],
            "tags": args.tags or [],
            "text": text,
            "payload": payload,
        }
        event_hash = _event_hash(event_core)
        event = {
            **event_core,
            "id": generate_ulid(),
            "hash": event_hash,
        }
        append_event(paths["events"], event)
        try:
            with conn:
                _insert_event(conn, event, dedupe_key, created_at)
        except DatabaseError:
            result.failed += 1
            result.errors.append(f"SQLite insert failed at idx {idx}")
            continue
        result.new += 1
    conn.close()
    if args.json:
        payload = {
            "adapter": adapter_name,
            "source_path": locator,
            "new": result.new,
            "skipped": result.skipped,
            "failed": result.failed,
            "errors": result.errors or [],
        }
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(
            f"Ingested: {result.new}  Skipped(deduped): {result.skipped}  Failed: {result.failed}"
        )
        print(f"Source: {locator}")
        print("Adapter: chat_json")
        print(f"Tags: {args.tags or []}")
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    conditions = ["events_fts MATCH ?"]
    params: list[Any] = [args.query]
    if args.type:
        conditions.append("e.type = ?")
        params.append(args.type)
    if args.after:
        conditions.append("e.ts >= ?")
        params.append(args.after)
    if args.before:
        conditions.append("e.ts <= ?")
        params.append(args.before)
    if args.tag:
        conditions.append("e.tags_json LIKE ?")
        params.append(f'%"{args.tag}"%')
    where_clause = " AND ".join(conditions)
    limit = args.limit or 20
    snippet_len = config.max_snippet_len
    query = (
        "SELECT e.id, e.ts, e.type, e.tags_json, "
        "substr(e.text, 1, ?) AS snippet "
        "FROM events_fts JOIN events e ON events_fts.rowid = e.rowid "
        f"WHERE {where_clause} "
        "LIMIT ?"
    )
    params_with_limit = [snippet_len] + params + [limit]
    try:
        rows = conn.execute(query, params_with_limit).fetchall()
    except sqlite3.Error as exc:
        raise DatabaseError(f"SQLite query error: {exc}") from exc
    if not rows:
        fallback_conditions = ["e.text LIKE ?"]
        fallback_params: list[Any] = [f"%{args.query}%"]
        if args.type:
            fallback_conditions.append("e.type = ?")
            fallback_params.append(args.type)
        if args.after:
            fallback_conditions.append("e.ts >= ?")
            fallback_params.append(args.after)
        if args.before:
            fallback_conditions.append("e.ts <= ?")
            fallback_params.append(args.before)
        if args.tag:
            fallback_conditions.append("e.tags_json LIKE ?")
            fallback_params.append(f'%\"{args.tag}\"%')
        fallback_where = " AND ".join(fallback_conditions)
        fallback_query = (
            "SELECT e.id, e.ts, e.type, e.tags_json, "
            "substr(e.text, 1, ?) AS snippet "
            "FROM events e "
            f"WHERE {fallback_where} "
            "LIMIT ?"
        )
        fallback_params = [snippet_len] + fallback_params + [limit]
        try:
            rows = conn.execute(fallback_query, fallback_params).fetchall()
        except sqlite3.Error as exc:
            raise DatabaseError(f"SQLite query error: {exc}") from exc
    results = []
    for row in rows:
        results.append(
            {
                "id": row["id"],
                "ts": row["ts"],
                "type": row["type"],
                "tags": json.loads(row["tags_json"]),
                "snippet": row["snippet"],
            }
        )
    conn.close()
    if args.json:
        print(json.dumps(results, ensure_ascii=False))
    else:
        for item in results:
            print(f"{item['id']} {item['ts']} {item['type']} {item['snippet']}")
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    conn = connect(paths["db"])
    row = conn.execute("SELECT * FROM events WHERE id = ?", (args.event_id,)).fetchone()
    if not row:
        conn.close()
        raise DatabaseError(f"Event not found: {args.event_id}")
    refs_rows = conn.execute("SELECT * FROM refs WHERE event_id = ?", (args.event_id,)).fetchall()
    refs = []
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
    }
    conn.close()
    if args.open:
        ref0 = refs[0] if refs else None
        if ref0 and ref0["uri"].startswith("file:"):
            file_path = ref0["uri"][5:]
            if sys.platform.startswith("darwin"):
                subprocess.run(["open", file_path], check=False)
            elif os.name == "nt":
                subprocess.run(["start", file_path], shell=True, check=False)
            else:
                subprocess.run(["xdg-open", file_path], check=False)
    if args.json:
        print(json.dumps(event, ensure_ascii=False))
    else:
        print(json.dumps(event, ensure_ascii=False, indent=2))
    return 0


def cmd_rebuild(args: argparse.Namespace) -> int:
    config = load_config(Path("ops.yml"))
    paths = _workspace_paths(config)
    events_path = Path(args.from_path) if args.from_path else paths["events"]
    if args.wipe:
        if paths["db"].exists():
            paths["db"].unlink()
        init_db(paths["db"])
    conn = connect(paths["db"])
    processed = 0
    inserted = 0
    skipped = 0
    parse_errors = 0
    with events_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            processed += 1
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                parse_errors += 1
                continue
            existing = conn.execute("SELECT 1 FROM events WHERE id = ?", (event.get("id"),)).fetchone()
            if existing:
                skipped += 1
                continue
            try:
                with conn:
                    _insert_event(
                        conn,
                        event,
                        event.get("dedupe_key"),
                        event.get("ts"),
                    )
                inserted += 1
            except DatabaseError:
                parse_errors += 1
                continue
    conn.close()
    print("Rebuilt index.")
    print(f"Events processed: {processed}")
    print(f"Inserted: {inserted}  Skipped(existing): {skipped}  Parse errors: {parse_errors}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ops")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("init")

    ingest_parser = subparsers.add_parser("ingest")
    ingest_sub = ingest_parser.add_subparsers(dest="adapter")
    chat_parser = ingest_sub.add_parser("chat_json")
    chat_parser.add_argument("path")
    chat_parser.add_argument("--tag", dest="tags", action="append")
    chat_parser.add_argument("--copy", dest="copy", action="store_true")
    chat_parser.add_argument("--no-copy", dest="copy", action="store_false")
    chat_parser.set_defaults(copy=True)
    chat_parser.add_argument("--json", action="store_true")

    query_parser = subparsers.add_parser("query")
    query_parser.add_argument("query")
    query_parser.add_argument("--type")
    query_parser.add_argument("--tag")
    query_parser.add_argument("--after")
    query_parser.add_argument("--before")
    query_parser.add_argument("--limit", type=int)
    query_parser.add_argument("--json", action="store_true")

    show_parser = subparsers.add_parser("show")
    show_parser.add_argument("event_id")
    show_parser.add_argument("--open", action="store_true")
    show_parser.add_argument("--json", action="store_true")

    rebuild_parser = subparsers.add_parser("index")
    rebuild_sub = rebuild_parser.add_subparsers(dest="action")
    rebuild_cmd = rebuild_sub.add_parser("rebuild")
    rebuild_cmd.add_argument("--from", dest="from_path")
    rebuild_cmd.add_argument("--wipe", dest="wipe", action="store_true")
    rebuild_cmd.add_argument("--no-wipe", dest="wipe", action="store_false")
    rebuild_cmd.set_defaults(wipe=True)

    return parser


def run(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help(sys.stderr)
        return 2
    if args.command == "init":
        return cmd_init(args)
    if args.command == "ingest":
        if args.adapter == "chat_json":
            return cmd_ingest(args)
        parser.print_help(sys.stderr)
        return 2
    if args.command == "query":
        return cmd_query(args)
    if args.command == "show":
        return cmd_show(args)
    if args.command == "index":
        if args.action == "rebuild":
            return cmd_rebuild(args)
        parser.print_help(sys.stderr)
        return 2
    raise AdapterError("Unknown command")


def main() -> None:
    try:
        code = run()
        raise SystemExit(code)
    except OpsError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(exc.exit_code) from exc
    except argparse.ArgumentError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(50) from exc
