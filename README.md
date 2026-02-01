# ops (Spec v0.2)

`ops` 是一个本地事件索引工具，使用 JSONL 作为 canonical source-of-truth，并使用 SQLite + FTS5 作为可重建索引。

## 快速开始

```bash
python -m ops init
python -m ops serve
python -m ops source add chat_export --path path/to/chat.json --tag demo --json
python -m ops ingest run chat_export --json
python -m ops search "demo" --json
python -m ops event show <event_id> --json
python -m ops job add index_rebuild --kind index_rebuild --config wipe=true --json
python -m ops job run index_rebuild --json
```

示例输出：

```text
Initialized workspace at data
canonical/events.jsonl OK
index/brain.sqlite OK
```

```json
{"new":3,"skipped":0,"failed":0,"errors":[]}
```

```json
[{"id":"01HV...","ts":"2026-01-21T10:00:00+09:00","type":"chat.message","tags":["demo"],"snippet":"..."}]
```

## 功能

- `ops init`：初始化工作目录结构、空 canonical 文件与 SQLite 索引。
- `ops serve`：启动本地 `opsd` 服务（默认 127.0.0.1:7777）。
- `ops source add|list|show|rm|test`：管理数据源（v0.2）。
- `ops ingest run <source_name>`：通过 opsd 运行 ingest。
- `ops ingest chat_json <path> --offline`：本地离线导入 JSON/JSONL 消息文件。
- `ops search <q>`：使用 FTS5 查询文本。
- `ops event show <event_id>`：查看完整事件结构。
- `ops job add|run`：运行任务（包含 `index_rebuild` 重建索引）。

## 说明

- Canonical 事件写入 `data/canonical/events.jsonl`，永不修改历史行。
- SQLite 索引位于 `data/index/brain.sqlite`，可随时删除并通过 rebuild 重建。
- CLI 默认通过 `opsd` 进行读写（`--endpoint` 可自定义）。离线 ingest 需显式 `--offline`。
- 多进程并发写入必须通过 `opsd` 或锁，禁止无锁并发写 canonical JSONL。
