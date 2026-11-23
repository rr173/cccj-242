"""分片文件(JSON)装载。

格式：
{
  "sender": "来件标识",
  "rows": [
    {"record": 1, "value": 100, "weight": 5},
    {"record": 2, "value": null, "weight": "9223372036854775815"}
  ]
}
value 为 null 表示未知值；weight 可用十进制字符串表示超大正整数。
"""

from __future__ import annotations

import json


def load_shard(path):
    """读取分片文件，返回 (sender, rows)；rows 为 (记录号, 估值, 权重) 列表。"""
    with open(path, "r", encoding="utf-8") as fh:
        doc = json.load(fh)
    if not isinstance(doc, dict) or not isinstance(doc.get("sender"), str):
        raise ValueError(f"分片文件缺少 sender: {path}")
    if not isinstance(doc.get("rows"), list):
        raise ValueError(f"分片文件缺少 rows: {path}")
    rows = [(r["record"], r["value"], r["weight"]) for r in doc["rows"]]
    return doc["sender"], rows
