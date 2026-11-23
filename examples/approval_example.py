#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""审批合并小例子：可共同出账与待协调两种结果（可反复运行）。

三位核算员从同一份转列批件出发：原批件台账与左、右两份各自试算成功的
修订稿台账（均由 build_ledger 生成）。merge_approvals 只读这三份台账，
按旧端副本位逐位比较三稿指向，产出两种互斥结果之一：

  场景一（可出账）：左稿把 A 的 [2,6) 改派、右稿撤回 B 的 [0,2)，两处
      修订互不冲突，交付一份可直接回灌 build_ledger 的合并清单。

  场景二（待协调）：
    - 旧端争议：A 的 [0,6) 左稿改派 D、右稿改派 F，两稿相对原批件都变
      且指向不同，不能自动合并（撤回对改派同理）；
    - 新端争议：左稿把 G 改派到 D[6,10)，右稿把 B 改派到 D[8,12)，
      合并后 D 的 [8,10) 被两个旧端副本位同时占用。
    此时只交付争议 JSONL（先旧端、后新端），不交付任何清单。

运行：python3 examples/approval_example.py（自带临时目录，结束即清理）
"""

import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ordered_exchange import (Dataset, build_ledger, merge_approvals,
                              STATUS_APPROVED, STATUS_DISPUTED)


def decl(old, new, length):
    return json.dumps({
        "old": {"sender": old[0], "record_no": old[1],
                "start": str(old[2])},
        "new": {"sender": new[0], "record_no": new[1],
                "start": str(new[2])},
        "length": str(length)}, ensure_ascii=False)


def build(tmp, old_db, new_db, lines, name):
    manifest = tmp / f"{name}.jsonl"
    manifest.write_text("\n".join(lines) + ("\n" if lines else ""),
                        encoding="utf-8")
    out = tmp / f"{name}.db"
    build_ledger(old_db, new_db, manifest, out)
    return out


def scenario_one(tmp):
    print("=" * 64)
    print("场景一：两份修订互不冲突 -> 可共同出账")
    print("=" * 64)
    old_db = tmp / "old1.db"
    new_db = tmp / "new1.db"
    Dataset.create(old_db, [("A", [(1, 1, 6)]), ("B", [(1, 1, 4)])]).close()
    Dataset.create(new_db, [("R", [(1, 1, 10)])]).close()
    base = build(tmp, old_db, new_db, [
        decl(("A", 1, 0), ("R", 1, 0), 6),
        decl(("B", 1, 0), ("R", 1, 6), 4)], "s1_base")
    # 左稿：A 的 [2,6) 改派到 R[2,6)（换号同址改派演示），B 不动
    left = build(tmp, old_db, new_db, [
        decl(("A", 1, 0), ("R", 1, 0), 2),
        decl(("A", 1, 2), ("R", 1, 2), 4),
        decl(("B", 1, 0), ("R", 1, 6), 4)], "s1_left")
    # 右稿：撤回 B 的 [0,2)（仅保留 B[2,4) 的声明），A 与原批件相同
    right = build(tmp, old_db, new_db, [
        decl(("A", 1, 0), ("R", 1, 0), 6),
        decl(("B", 1, 2), ("R", 1, 8), 2)], "s1_right")
    manifest_out = tmp / "s1_merged.jsonl"
    disputes_out = tmp / "s1_disputes.jsonl"
    summary = merge_approvals(base, left, right, manifest_out, disputes_out)
    print("摘要:", json.dumps(summary, ensure_ascii=False))
    assert summary["status"] == STATUS_APPROVED
    print("合并清单（可直接传给 build_ledger）:")
    print(manifest_out.read_text(encoding="utf-8").strip())
    # 回灌核算：合并清单是一份合法的转列清单
    rebuilt = tmp / "s1_rebuilt.db"
    build_ledger(old_db, new_db, manifest_out, rebuilt)
    print("回灌 build_ledger 成功 ->", rebuilt.name)
    print("争议明细未交付:", not disputes_out.exists())


def scenario_two(tmp):
    print("=" * 64)
    print("场景二：旧端争议与新端争议并存 -> 待协调")
    print("=" * 64)
    old_db = tmp / "old2.db"
    new_db = tmp / "new2.db"
    Dataset.create(old_db, [
        ("A", [(1, 1, 6)]), ("B", [(1, 1, 6)]),
        ("G", [(1, 1, 4)])]).close()
    Dataset.create(new_db, [
        ("D", [(1, 1, 12)]), ("F", [(1, 1, 6)])]).close()
    # 原批件：A[0,6) -> F[0,6)
    base = build(tmp, old_db, new_db, [
        decl(("A", 1, 0), ("F", 1, 0), 6)], "s2_base")
    # 左稿：A 改派 D[0,6)；G 改派 D[6,10)
    left = build(tmp, old_db, new_db, [
        decl(("A", 1, 0), ("D", 1, 0), 6),
        decl(("G", 1, 0), ("D", 1, 6), 4)], "s2_left")
    # 右稿：撤回 A 的声明；B 改派 D[8,12)
    right = build(tmp, old_db, new_db, [
        decl(("B", 1, 0), ("D", 1, 8), 4)], "s2_right")
    manifest_out = tmp / "s2_merged.jsonl"
    disputes_out = tmp / "s2_disputes.jsonl"
    summary = merge_approvals(base, left, right, manifest_out, disputes_out)
    print("摘要:", json.dumps(summary, ensure_ascii=False))
    assert summary["status"] == STATUS_DISPUTED
    print("争议明细（先旧端、后新端）:")
    for line in disputes_out.read_text(encoding="utf-8").splitlines():
        print(" ", line)
    print("清单未交付（含部分清单）:", not manifest_out.exists())


def main():
    tmp = Path(tempfile.mkdtemp(prefix="oappr_demo_"))
    try:
        scenario_one(tmp)
        print()
        scenario_two(tmp)
        print()
        print("两个场景均符合预期。")
    finally:
        shutil.rmtree(tmp, True)


if __name__ == "__main__":
    main()
