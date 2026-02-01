import json
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

import sqlite3

REPO_ROOT = Path(__file__).resolve().parents[1]
DEDUPE_RE = re.compile(r"^[0-9a-f]{64}$")


def run_ops(cwd: Path, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    merged_env = os.environ.copy()
    merged_env["PYTHONPATH"] = str(REPO_ROOT)
    if env:
        merged_env.update(env)
    return subprocess.run(
        [sys.executable, "-m", "ops", *args],
        cwd=cwd,
        env=merged_env,
        capture_output=True,
        text=True,
    )


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_health(host: str, port: int, timeout: float = 5.0) -> dict[str, Any]:
    start = time.monotonic()
    url = f"http://{host}:{port}/health"
    while time.monotonic() - start < timeout:
        try:
            with urlopen(url, timeout=0.5) as response:
                return json.loads(response.read().decode("utf-8"))
        except URLError:
            time.sleep(0.1)
    raise AssertionError("opsd health check timed out")


def http_post(host: str, port: int, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"http://{host}:{port}{path}"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=3.0) as response:
        return json.loads(response.read().decode("utf-8"))


def http_get(host: str, port: int, path: str) -> dict[str, Any]:
    url = f"http://{host}:{port}{path}"
    with urlopen(url, timeout=3.0) as response:
        return json.loads(response.read().decode("utf-8"))


def write_chat_small(path: Path) -> None:
    path.write_text(
        """[
{"ts":"2026-01-21T10:00:00+09:00","speaker":"user","content":"我想做 memobird CLI 打印","thread_id":"t1"},
{"ts":"2026-01-21T10:00:05+09:00","speaker":"assistant","content":"可以，先抓包再分析协议","thread_id":"t1"},
{"ts":"2026-01-21T10:00:10+09:00","speaker":"user","content":"对账也想自动化，导出支付宝微信","thread_id":"t1"}
]
""",
        encoding="utf-8",
    )


def start_opsd(tmp_path: Path) -> tuple[subprocess.Popen, int]:
    init_result = run_ops(tmp_path, "init")
    assert init_result.returncode == 0, init_result.stderr

    port = get_free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "ops", "serve", "--host", "127.0.0.1", "--port", str(port)],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    payload = wait_for_health("127.0.0.1", port)
    assert payload["ok"] is True
    assert payload["version"] == "0.2"
    assert payload["schema_version"] == "0.2"
    return proc, port


def test_sources_crud_and_ingest_run(tmp_path: Path) -> None:
    chat_small = tmp_path / "chat_small.json"
    write_chat_small(chat_small)

    proc, port = start_opsd(tmp_path)
    endpoint = f"http://127.0.0.1:{port}"
    try:
        add_result = run_ops(
            tmp_path,
            "source",
            "add",
            "chat_export",
            "--path",
            str(chat_small),
            "--tag",
            "chat",
            "--tag",
            "ai",
            "--endpoint",
            endpoint,
            "--json",
        )
        assert add_result.returncode == 0, add_result.stderr

        ingest_result = run_ops(
            tmp_path,
            "ingest",
            "run",
            "chat_export",
            "--endpoint",
            endpoint,
            "--json",
        )
        payload = json.loads(ingest_result.stdout)
        assert payload["new"] == 3
        assert payload["skipped"] == 0

        ingest_result2 = run_ops(
            tmp_path,
            "ingest",
            "run",
            "chat_export",
            "--endpoint",
            endpoint,
            "--json",
        )
        payload2 = json.loads(ingest_result2.stdout)
        assert payload2["new"] == 0
        assert payload2["skipped"] == 3

        events_response = http_get("127.0.0.1", port, "/v1/events?format=full")
        assert len(events_response["items"]) == 3
        for event in events_response["items"]:
            assert event.get("dedupe_key")
            assert DEDUPE_RE.match(event["dedupe_key"])

        canonical_path = tmp_path / "data" / "canonical" / "events.jsonl"
        canonical_events = read_jsonl(canonical_path)
        assert len(canonical_events) == 3
        for event in canonical_events:
            assert event.get("dedupe_key")
            assert DEDUPE_RE.match(event["dedupe_key"])
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_views(tmp_path: Path) -> None:
    chat_small = tmp_path / "chat_small.json"
    write_chat_small(chat_small)
    proc, port = start_opsd(tmp_path)
    endpoint = f"http://127.0.0.1:{port}"
    try:
        run_ops(
            tmp_path,
            "source",
            "add",
            "chat_export",
            "--path",
            str(chat_small),
            "--endpoint",
            endpoint,
            "--json",
        )
        run_ops(tmp_path, "ingest", "run", "chat_export", "--endpoint", endpoint, "--json")

        create_view = http_post(
            "127.0.0.1",
            port,
            "/v1/views",
            {
                "name": "chat_timeline",
                "description": "chat asc",
                "query": {"kind": "events_query", "filters": {"type": ["chat.message"]}, "order": "asc"},
            },
        )
        assert create_view["name"] == "chat_timeline"

        query_view = http_post(
            "127.0.0.1",
            port,
            "/v1/views/chat_timeline:query",
            {"filters": {}, "limit": 10},
        )
        items = query_view["items"]
        assert len(items) == 3
        assert items[0]["ts"] <= items[1]["ts"] <= items[2]["ts"]
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_jobs_daily_digest_creates_artifact(tmp_path: Path) -> None:
    chat_small = tmp_path / "chat_small.json"
    write_chat_small(chat_small)
    proc, port = start_opsd(tmp_path)
    endpoint = f"http://127.0.0.1:{port}"
    try:
        run_ops(
            tmp_path,
            "source",
            "add",
            "chat_export",
            "--path",
            str(chat_small),
            "--endpoint",
            endpoint,
            "--json",
        )
        run_ops(tmp_path, "ingest", "run", "chat_export", "--endpoint", endpoint, "--json")

        job_config = {
            "view": "timeline",
            "day": "2026-01-21",
            "out_dir": "artifacts/runs/2026-01-21",
            "tags": ["memobird"],
        }
        run_ops(
            tmp_path,
            "job",
            "add",
            "daily_digest",
            "--kind",
            "daily_digest",
            "--config",
            json.dumps(job_config, ensure_ascii=False),
            "--endpoint",
            endpoint,
            "--json",
        )

        run_result = run_ops(
            tmp_path,
            "job",
            "run",
            "daily_digest",
            "--endpoint",
            endpoint,
            "--json",
        )
        payload = json.loads(run_result.stdout)
        assert payload["status"] == "ok"

        digest_path = tmp_path / "data" / "artifacts" / "runs" / "2026-01-21" / "daily_digest.md"
        assert digest_path.exists()

        events_response = http_get("127.0.0.1", port, "/v1/events?type=artifact.created&format=full")
        refs = [
            ref
            for event in events_response["items"]
            for ref in event.get("refs", [])
            if ref.get("uri") == f"file:{digest_path}"
        ]
        assert refs
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_artifacts_pack(tmp_path: Path) -> None:
    chat_small = tmp_path / "chat_small.json"
    write_chat_small(chat_small)
    proc, port = start_opsd(tmp_path)
    endpoint = f"http://127.0.0.1:{port}"
    try:
        run_ops(
            tmp_path,
            "source",
            "add",
            "chat_export",
            "--path",
            str(chat_small),
            "--endpoint",
            endpoint,
            "--json",
        )
        run_ops(tmp_path, "ingest", "run", "chat_export", "--endpoint", endpoint, "--json")

        job_config = {
            "view": "timeline",
            "day": "2026-01-21",
            "out_dir": "artifacts/runs/2026-01-21",
            "tags": ["memobird"],
        }
        run_ops(
            tmp_path,
            "job",
            "add",
            "daily_digest",
            "--kind",
            "daily_digest",
            "--config",
            json.dumps(job_config, ensure_ascii=False),
            "--endpoint",
            endpoint,
            "--json",
        )
        run_ops(tmp_path, "job", "run", "daily_digest", "--endpoint", endpoint, "--json")

        pack_response = http_post(
            "127.0.0.1",
            port,
            "/v1/artifacts:pack",
            {"tag": "memobird", "out_dir": "artifacts/packs/memobird"},
        )
        pack_path = tmp_path / "data" / "artifacts" / "packs" / "memobird" / "pack.json"
        readme_path = tmp_path / "data" / "artifacts" / "packs" / "memobird" / "README.md"
        assert Path(pack_response["pack_path"]).exists()
        assert pack_path.exists()
        assert readme_path.exists()

        events_response = http_get("127.0.0.1", port, "/v1/events?type=artifact.created&format=full")
        refs = [
            ref
            for event in events_response["items"]
            for ref in event.get("refs", [])
            if ref.get("uri") in {f"file:{pack_path}", f"file:{readme_path}"}
        ]
        assert refs
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_cli_offline_ingest(tmp_path: Path) -> None:
    chat_small = tmp_path / "chat_small.json"
    write_chat_small(chat_small)
    init_result = run_ops(tmp_path, "init")
    assert init_result.returncode == 0, init_result.stderr

    ingest_result = run_ops(
        tmp_path,
        "ingest",
        "chat_json",
        str(chat_small),
        "--tag",
        "memobird",
        "--offline",
        "--json",
    )
    assert ingest_result.returncode == 0, ingest_result.stderr
    payload = json.loads(ingest_result.stdout)
    assert payload["new"] == 3

    canonical_path = tmp_path / "data" / "canonical" / "events.jsonl"
    assert len(read_jsonl(canonical_path)) == 3

    db_path = tmp_path / "data" / "index" / "brain.sqlite"
    conn = sqlite3.connect(db_path)
    events_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    conn.close()
    assert events_count == 3

    ingest_result2 = run_ops(
        tmp_path,
        "ingest",
        "chat_json",
        str(chat_small),
        "--tag",
        "memobird",
        "--offline",
        "--json",
    )
    payload2 = json.loads(ingest_result2.stdout)
    assert payload2["new"] == 0
    assert len(read_jsonl(canonical_path)) == 3


def test_index_rebuild_job_recovers_index(tmp_path: Path) -> None:
    chat_small = tmp_path / "chat_small.json"
    write_chat_small(chat_small)
    proc, port = start_opsd(tmp_path)
    endpoint = f"http://127.0.0.1:{port}"
    try:
        run_ops(
            tmp_path,
            "source",
            "add",
            "chat_export",
            "--path",
            str(chat_small),
            "--endpoint",
            endpoint,
            "--json",
        )
        run_ops(tmp_path, "ingest", "run", "chat_export", "--endpoint", endpoint, "--json")

        db_path = tmp_path / "data" / "index" / "brain.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("DELETE FROM refs")
        conn.execute("DELETE FROM dedupe")
        conn.execute("DELETE FROM events")
        conn.execute("DELETE FROM events_fts")
        conn.commit()
        conn.close()

        empty_events = http_get("127.0.0.1", port, "/v1/events?format=summary")
        assert empty_events["items"] == []

        job_config = {"wipe": True, "fts": True}
        run_ops(
            tmp_path,
            "job",
            "add",
            "index_rebuild",
            "--kind",
            "index_rebuild",
            "--config",
            json.dumps(job_config, ensure_ascii=False),
            "--endpoint",
            endpoint,
            "--json",
        )
        run_ops(tmp_path, "job", "run", "index_rebuild", "--endpoint", endpoint, "--json")

        events_response = http_get("127.0.0.1", port, "/v1/events?format=full")
        assert len(events_response["items"]) == 4
        for event in events_response["items"]:
            if event["type"] == "chat.message":
                assert event.get("dedupe_key")
                assert DEDUPE_RE.match(event["dedupe_key"])

        search_response = http_get("127.0.0.1", port, "/v1/events?q=memobird")
        assert search_response["items"]

        canonical_path = tmp_path / "data" / "canonical" / "events.jsonl"
        assert len(read_jsonl(canonical_path)) == 4
    finally:
        proc.terminate()
        proc.wait(timeout=5)
