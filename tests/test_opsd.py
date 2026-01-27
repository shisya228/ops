import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

import sqlite3

REPO_ROOT = Path(__file__).resolve().parents[1]


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


def post_batch(host: str, port: int, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"http://{host}:{port}/v1/events:batch"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=2.0) as response:
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


def build_drafts(locator: str, tags: list[str]) -> list[dict[str, Any]]:
    payloads = [
        ("2026-01-21T10:00:00+09:00", "user", "我想做 memobird CLI 打印", "t1"),
        ("2026-01-21T10:00:05+09:00", "assistant", "可以，先抓包再分析协议", "t1"),
        ("2026-01-21T10:00:10+09:00", "user", "对账也想自动化，导出支付宝微信", "t1"),
    ]
    drafts = []
    for idx, (ts, speaker, content, thread_id) in enumerate(payloads):
        drafts.append(
            {
                "schema_version": "0.1",
                "ts": ts,
                "type": "chat.message",
                "source": {"kind": "chat_json", "locator": locator, "meta": {}},
                "refs": [{"kind": "file", "uri": f"file:{locator}", "span": {"idx": idx}}],
                "tags": tags,
                "text": content,
                "payload": {"speaker": speaker, "content": content, "thread_id": thread_id},
            }
        )
    return drafts


def test_opsd_health(tmp_path: Path) -> None:
    init_result = run_ops(tmp_path, "init")
    assert init_result.returncode == 0, init_result.stderr

    port = get_free_port()
    env = {"OPSD_PORT": str(port)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "ops", "daemon", "start", "--host", "127.0.0.1", "--port", str(port)],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT), **env},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        payload = wait_for_health("127.0.0.1", port)
        assert payload["ok"] is True
        assert payload["version"] == "0.1"
        assert Path(payload["workspace"]).resolve() == (tmp_path / "data").resolve()
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_opsd_batch_dedupe(tmp_path: Path) -> None:
    init_result = run_ops(tmp_path, "init")
    assert init_result.returncode == 0, init_result.stderr
    chat_small = tmp_path / "chat_small.json"
    write_chat_small(chat_small)
    locator = str(chat_small)

    port = get_free_port()
    env = {"OPSD_PORT": str(port)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "ops", "daemon", "start", "--host", "127.0.0.1", "--port", str(port)],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT), **env},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_health("127.0.0.1", port)
        drafts = build_drafts(locator, ["demo"])
        response1 = post_batch("127.0.0.1", port, {"events": drafts, "atomic": False})
        assert response1["inserted"] == 3
        assert response1["skipped"] == 0
        response2 = post_batch("127.0.0.1", port, {"events": drafts, "atomic": False})
        assert response2["inserted"] == 0
        assert response2["skipped"] == 3
        for item in response2["results"]:
            assert item["status"] == "skipped"
            assert item.get("existing_event_id")

        canonical_path = tmp_path / "data" / "canonical" / "events.jsonl"
        events = read_jsonl(canonical_path)
        assert len(events) == 3
        db_path = tmp_path / "data" / "index" / "brain.sqlite"
        conn = sqlite3.connect(db_path)
        events_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        dedupe_count = conn.execute("SELECT COUNT(*) FROM dedupe").fetchone()[0]
        conn.close()
        assert events_count == 3
        assert dedupe_count == 3
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_opsd_concurrent_requests(tmp_path: Path) -> None:
    init_result = run_ops(tmp_path, "init")
    assert init_result.returncode == 0, init_result.stderr

    locator = str(tmp_path / "chat_small.json")
    draft = {
        "schema_version": "0.1",
        "ts": "2026-01-21T10:00:00+09:00",
        "type": "chat.message",
        "source": {"kind": "chat_json", "locator": locator, "meta": {}},
        "refs": [{"kind": "file", "uri": f"file:{locator}", "span": {"idx": 0}}],
        "tags": ["demo"],
        "text": "并发测试",
        "payload": {"speaker": "user", "content": "并发测试", "thread_id": "t1"},
    }

    port = get_free_port()
    env = {"OPSD_PORT": str(port)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "ops", "daemon", "start", "--host", "127.0.0.1", "--port", str(port)],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT), **env},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_health("127.0.0.1", port)
        threads = []
        for _ in range(20):
            thread = threading.Thread(
                target=post_batch,
                args=("127.0.0.1", port, {"events": [draft], "atomic": False}),
            )
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

        canonical_path = tmp_path / "data" / "canonical" / "events.jsonl"
        events = read_jsonl(canonical_path)
        assert len(events) == 1
        db_path = tmp_path / "data" / "index" / "brain.sqlite"
        conn = sqlite3.connect(db_path)
        events_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        dedupe_count = conn.execute("SELECT COUNT(*) FROM dedupe").fetchone()[0]
        conn.close()
        assert events_count == 1
        assert dedupe_count == 1
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_cli_ingest_prefers_opsd(tmp_path: Path) -> None:
    init_result = run_ops(tmp_path, "init")
    assert init_result.returncode == 0, init_result.stderr
    chat_small = tmp_path / "chat_small.json"
    write_chat_small(chat_small)

    port = get_free_port()
    env = {"OPSD_PORT": str(port)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "ops", "daemon", "start", "--host", "127.0.0.1", "--port", str(port)],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT), **env},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_health("127.0.0.1", port)
        ingest_result = run_ops(
            tmp_path,
            "ingest",
            "chat_json",
            str(chat_small),
            "--json",
            env={"OPSD_PORT": str(port), "OPS_USE_DAEMON": "1"},
        )
        assert ingest_result.returncode == 0, ingest_result.stderr
        payload = json.loads(ingest_result.stdout)
        assert payload["new"] == 3
        assert payload["skipped"] == 0

        ingest_result2 = run_ops(
            tmp_path,
            "ingest",
            "chat_json",
            str(chat_small),
            "--json",
            env={"OPSD_PORT": str(port), "OPS_USE_DAEMON": "1"},
        )
        payload2 = json.loads(ingest_result2.stdout)
        assert payload2["new"] == 0
        assert payload2["skipped"] == 3
    finally:
        proc.terminate()
        proc.wait(timeout=5)
