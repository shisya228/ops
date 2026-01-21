import json
import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
ULID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")


def run_ops(cwd: Path, *args: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    return subprocess.run(
        [sys.executable, "-m", "ops", *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
    )


def write_file(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_init_ingest_query_show_rebuild(tmp_path: Path) -> None:
    chat_small = tmp_path / "chat_small.json"
    write_file(
        chat_small,
        """[
{"ts":"2026-01-21T10:00:00+09:00","speaker":"user","content":"我想做 memobird CLI 打印","thread_id":"t1"},
{"ts":"2026-01-21T10:00:05+09:00","speaker":"assistant","content":"可以，先抓包再分析协议","thread_id":"t1"},
{"ts":"2026-01-21T10:00:10+09:00","speaker":"user","content":"对账也想自动化，导出支付宝微信","thread_id":"t1"}
]
""",
    )

    init_result = run_ops(tmp_path, "init")
    assert init_result.returncode == 0, init_result.stderr

    ingest_result = run_ops(
        tmp_path,
        "ingest",
        "chat_json",
        str(chat_small),
        "--tag",
        "t2",
        "--tag",
        "memobird",
        "--json",
    )
    assert ingest_result.returncode == 0, ingest_result.stderr
    payload = json.loads(ingest_result.stdout)
    assert payload["new"] == 3
    assert payload["skipped"] == 0
    assert payload["failed"] == 0

    canonical_path = tmp_path / "data" / "canonical" / "events.jsonl"
    events = read_jsonl(canonical_path)
    assert len(events) == 3

    db_path = tmp_path / "data" / "index" / "brain.sqlite"
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT id, type, tags_json FROM events").fetchall()
    assert len(rows) == 3
    for row in rows:
        assert row[1] == "chat.message"
        tags = json.loads(row[2])
        assert "t2" in tags and "memobird" in tags
    refs_rows = conn.execute("SELECT ref_kind, uri, span_json FROM refs ORDER BY id").fetchall()
    assert refs_rows[0][0] == "file"
    assert refs_rows[0][1].startswith("file:")
    spans = [json.loads(row[2]) for row in refs_rows]
    assert spans == [{"idx": 0}, {"idx": 1}, {"idx": 2}]
    conn.close()

    for event in events:
        assert ULID_RE.match(event["id"])
        assert event["hash"]["algo"] == "sha256"
        assert len(event["hash"]["value"]) == 64

    ingest_result2 = run_ops(
        tmp_path,
        "ingest",
        "chat_json",
        str(chat_small),
        "--tag",
        "t2",
        "--tag",
        "memobird",
        "--json",
    )
    payload2 = json.loads(ingest_result2.stdout)
    assert payload2["new"] == 0
    assert payload2["skipped"] == 3
    assert payload2["failed"] == 0

    events_after = read_jsonl(canonical_path)
    assert len(events_after) == 3

    conn = sqlite3.connect(db_path)
    events_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    dedupe_count = conn.execute("SELECT COUNT(*) FROM dedupe").fetchone()[0]
    assert events_count == 3
    assert dedupe_count == 3
    conn.close()

    query_result = run_ops(tmp_path, "query", "memobird", "--json")
    assert query_result.returncode == 0
    results = json.loads(query_result.stdout)
    assert results
    assert any("memobird" in item["snippet"] for item in results)

    event_id = results[0]["id"]
    show_result = run_ops(tmp_path, "show", event_id, "--json")
    assert show_result.returncode == 0
    show_payload = json.loads(show_result.stdout)
    for key in [
        "schema_version",
        "id",
        "ts",
        "type",
        "source",
        "refs",
        "tags",
        "text",
        "payload",
        "hash",
    ]:
        assert key in show_payload
    assert show_payload["payload"]["content"] == "我想做 memobird CLI 打印"

    rebuild_result = run_ops(tmp_path, "index", "rebuild", "--wipe")
    assert rebuild_result.returncode == 0
    assert "Events processed: 3" in rebuild_result.stdout
    assert "Inserted: 3" in rebuild_result.stdout
    assert "Parse errors: 0" in rebuild_result.stdout

    query_result2 = run_ops(tmp_path, "query", "memobird", "--json")
    results2 = json.loads(query_result2.stdout)
    assert results2


def test_ingest_jsonl_and_query(tmp_path: Path) -> None:
    chat_small = tmp_path / "chat_small.jsonl"
    write_file(
        chat_small,
        """{"ts":"2026-01-21T11:00:00+09:00","speaker":"user","content":"AST 污点分析怎么做","thread_id":"t2"}
{"ts":"2026-01-21T11:00:05+09:00","speaker":"assistant","content":"先做调用图，再做source-sink路径","thread_id":"t2"}
""",
    )

    init_result = run_ops(tmp_path, "init")
    assert init_result.returncode == 0, init_result.stderr

    ingest_result = run_ops(tmp_path, "ingest", "chat_json", str(chat_small), "--json")
    assert ingest_result.returncode == 0
    payload = json.loads(ingest_result.stdout)
    assert payload["new"] == 2
    assert payload["skipped"] == 0

    query_result = run_ops(tmp_path, "query", "调用图", "--json")
    assert query_result.returncode == 0
    results = json.loads(query_result.stdout)
    assert results
