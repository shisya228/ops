# ops (Spec v0.1)

`ops` 是一个本地事件索引工具，使用 JSONL 作为 canonical source-of-truth，并使用 SQLite + FTS5 作为可重建索引。

## 快速开始

```bash
python -m ops init
python -m ops ingest chat_json path/to/chat.json --tag demo --json
python -m ops query "demo" --json
python -m ops show <event_id> --json
python -m ops index rebuild --wipe
```

示例输出：

```text
Initialized workspace at data
canonical/events.jsonl OK
index/brain.sqlite OK
```

```json
{"adapter":"chat_json","source_path":"data/raw/chat_json/abcd1234_chat.json","new":3,"skipped":0,"failed":0,"errors":[]}
```

```json
[{"id":"01HV...","ts":"2026-01-21T10:00:00+09:00","type":"chat.message","tags":["demo"],"snippet":"..."}]
```

## 功能

- `ops init`：初始化工作目录结构、空 canonical 文件与 SQLite 索引。
- `ops ingest chat_json <path>`：导入 JSON 数组或 JSONL 消息文件，支持去重与可重建索引。
- `ops query <q>`：使用 FTS5 查询文本。
- `ops show <event_id>`：查看完整事件结构。
- `ops index rebuild`：从 `events.jsonl` 重建 SQLite 索引。

## 说明

- Canonical 事件写入 `data/canonical/events.jsonl`，永不修改历史行。
- SQLite 索引位于 `data/index/brain.sqlite`，可随时删除并通过 rebuild 重建。
