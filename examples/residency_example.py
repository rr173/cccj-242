#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""驻留资格判定小例子：同一条请求被拆成不同结论（可反复运行）。

场景：地区 east / west，第 0、1、2 期共两份台账。末期 east 记录 1 权重 10：

  [0, 4)   第 0 期 east 记录 5 同地区转列为第 1 期 east 记录 1（换号不重新
           计时），第 1→2 期默认承接 —— 同地区前身能追到第 0 期，下限 3 期；
  [4, 10)  第 1→2 期才从 west 记录 2 跨地区转列进来 —— 驻留自第 2 期起算，
           确切 1 期。

请求 {"sender": "east", "record_no": 1, "start": "0", "length": "10"} 在最低
连续 3 期下被拆成两段：[0,4) 符合（下限 3 期）+ [4,10) 不符合（确切 1 期）。

运行：python3 examples/residency_example.py（自带临时目录，结束即清理）
"""

import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ordered_exchange import Dataset, build_ledger, residency_report


def decl(o, n, length):
    return json.dumps({
        "old": {"sender": o[0], "record_no": o[1], "start": str(o[2])},
        "new": {"sender": n[0], "record_no": n[1], "start": str(n[2])},
        "length": str(length)}, ensure_ascii=False)


def main():
    tmp = Path(tempfile.mkdtemp(prefix="ores_demo_"))
    try:
        # 三个关账期的数据集
        Dataset.create(tmp / "d0.db", [("east", [(5, 3, 4)])]).close()
        Dataset.create(tmp / "d1.db", [("east", [(1, 3, 4)]),
                                       ("west", [(2, 8, 6)])]).close()
        Dataset.create(tmp / "d2.db", [("east", [(1, 3, 10)])]).close()

        # 第 0→1 期台账：east 记录 5 [0,4) 同地区转列为 east 记录 1 [0,4)
        (tmp / "m1.jsonl").write_text(
            decl(("east", 5, 0), ("east", 1, 0), 4) + "\n", encoding="utf-8")
        build_ledger(tmp / "d0.db", tmp / "d1.db", tmp / "m1.jsonl",
                     tmp / "l1.db")
        # 第 1→2 期台账：west 记录 2 [0,6) 跨地区转列为 east 记录 1 [4,10)
        (tmp / "m2.jsonl").write_text(
            decl(("west", 2, 0), ("east", 1, 4), 6) + "\n", encoding="utf-8")
        build_ledger(tmp / "d1.db", tmp / "d2.db", tmp / "m2.jsonl",
                     tmp / "l2.db")

        # 末期待核请求：east 记录 1 全段 [0,10)，最低连续 3 期
        (tmp / "requests.jsonl").write_text(
            json.dumps({"sender": "east", "record_no": 1,
                        "start": "0", "length": "10"}) + "\n",
            encoding="utf-8")
        summary = residency_report([tmp / "l1.db", tmp / "l2.db"], 3,
                                   tmp / "requests.jsonl",
                                   tmp / "report.jsonl")
        print("请求: (east, 1, [0, 10))，最低连续 3 期 ->")
        for line in (tmp / "report.jsonl").read_text(
                encoding="utf-8").splitlines():
            print(line)
        print("摘要:", json.dumps(summary, ensure_ascii=False))
    finally:
        shutil.rmtree(tmp, True)


if __name__ == "__main__":
    main()
