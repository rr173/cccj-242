# -*- coding: utf-8 -*-
"""驻留资格判定的测试。

含一个逐副本展开的独立参照模型：从台账明细展开每期映射，逐副本回溯驻留
起点并按合并规则分组，与 residency_report 的交付文件全量对照。
"""

import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ordered_exchange import (Dataset, Ledger, build_ledger, residency_report,
                              ResidencyError)


# ------------------------------------------------------------ 参照模型

class ResidencyModel:
    """逐副本展开的独立参照（仅适用小权重）。

    从各份台账的明细展开每期映射，逐副本回溯驻留起点。
    """

    def __init__(self, ledger_paths):
        self.n = len(ledger_paths)
        self.steps = []  # 每份台账: {(sender, rec, pos): action}
        for p in ledger_paths:
            m = {}
            with Ledger(p) as led:
                for e in led.entries():
                    if e["kind"] == "exit":
                        continue
                    nk = (e["new"]["sender"], e["new"]["record_no"])
                    ns = e["new"]["start"]
                    for k in range(e["length"]):
                        if e["kind"] == "new":
                            m[(nk[0], nk[1], ns + k)] = ("new", e["entry_id"])
                        elif e["kind"] == "carry":
                            m[(nk[0], nk[1], ns + k)] = ("carry", ns + k)
                        else:  # transfer
                            m[(nk[0], nk[1], ns + k)] = (
                                "transfer", e["old"]["sender"],
                                e["old"]["record_no"],
                                e["old"]["start"] + k, e["entry_id"])
            self.steps.append(m)

    def trace(self, sender, rec, pos):
        """单个副本的驻留结论：
        (exact, periods, ev_period, ev_sender, ev_rec, ev_start, entry_id)。"""
        for level in range(self.n, 0, -1):
            a = self.steps[level - 1][(sender, rec, pos)]
            if a[0] == "new":
                return (True, self.n - level + 1, level, sender, rec, pos,
                        a[1])
            if a[0] == "carry":
                continue
            _, o_s, o_r, o_p, eid = a
            if o_s != sender:
                return (True, self.n - level + 1, level, sender, rec, pos,
                        eid)
            sender, rec, pos = o_s, o_r, o_p
        return (False, self.n + 1, 0, sender, rec, pos, None)

    def expected(self, line_no, sender, rec, start, length, min_periods):
        """一个请求的期望输出段（按合并规则分组）。"""
        segs = []
        for pos in range(start, start + length):
            exact, count, ep, es, er, estart, eid = self.trace(
                sender, rec, pos)
            if (segs and segs[-1]["exact"] == exact
                    and segs[-1]["basis"]["period"] == ep
                    and segs[-1]["basis"]["sender"] == es
                    and segs[-1]["basis"]["record_no"] == er
                    and segs[-1]["basis"].get("entry_id") == eid
                    and int(segs[-1]["basis"]["start"])
                    + int(segs[-1]["length"]) == estart):
                segs[-1]["length"] = str(int(segs[-1]["length"]) + 1)
                continue
            if count >= min_periods:
                verdict = "符合"
            elif exact:
                verdict = "不符合"
            else:
                verdict = "历史不足"
            basis = {"period": ep, "sender": es, "record_no": er,
                     "start": str(estart)}
            if eid is not None:
                basis["entry_id"] = eid
            segs.append({"line_no": line_no, "sender": sender,
                         "record_no": rec, "start": str(pos), "length": "1",
                         "verdict": verdict, "periods": count,
                         "exact": exact, "basis": basis})
        return segs


# ------------------------------------------------------------ 工具

def rows_to_shards(rows):
    """{(sender, rec): (value, weight)} -> Dataset.create 可用的分片列表。"""
    by_sender = {}
    for (s, r), (v, w) in rows.items():
        by_sender.setdefault(s, []).append((r, v, w))
    shards = []
    for s in sorted(by_sender):
        rs = sorted(by_sender[s],
                    key=lambda t: (t[1] is None,
                                   t[1] if t[1] is not None else 0))
        shards.append((s, rs))
    return shards


def decl_json(o, n, length):
    return json.dumps({
        "old": {"sender": o[0], "record_no": o[1], "start": str(o[2])},
        "new": {"sender": n[0], "record_no": n[1], "start": str(n[2])},
        "length": str(length)}, ensure_ascii=False)


def req_json(sender, rec, start, length):
    return json.dumps({"sender": sender, "record_no": rec,
                       "start": str(start), "length": str(length)},
                      ensure_ascii=False)


# ------------------------------------------------------------ 测试基类

class ResidencyCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="ores_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def path(self, name):
        return self.tmp / name

    def make_dataset(self, name, shards):
        p = self.path(name)
        Dataset.create(p, shards).close()
        return p

    def build(self, old, new, decls, name):
        m = self.path(name + ".manifest")
        with open(m, "w", encoding="utf-8") as fh:
            for d in decls:
                fh.write(d + "\n")
        out = self.path(name)
        build_ledger(old, new, m, out)
        return out

    def write_requests(self, lines, name="requests.jsonl"):
        p = self.path(name)
        with open(p, "w", encoding="utf-8") as fh:
            for ln in lines:
                fh.write(ln if ln.endswith("\n") else ln + "\n")
        return p

    def run_report(self, ledgers, min_periods, requests, out="report.jsonl"):
        out = self.path(out)
        summary = residency_report(ledgers, min_periods, requests, out)
        lines = []
        with open(out, encoding="utf-8") as fh:
            for ln in fh:
                lines.append(json.loads(ln))
        self.assertEqual(self.tmp_files(), [])  # 不留临时文件
        return summary, lines

    def tmp_files(self):
        return [f for f in os.listdir(self.tmp) if f.endswith(".tmp")]

    def entry_ids(self, ledger_path, kind):
        with Ledger(ledger_path) as led:
            return [e["entry_id"] for e in led.entries(kind)]


# ------------------------------------------------------------ 共用期一致性

class ChainConsistencyTest(ResidencyCase):
    def make_chain(self, d1_alt_shards=None, tag=""):
        d0 = self.make_dataset(f"d0{tag}.db", [("a", [(1, 5, 4)])])
        d1 = self.make_dataset(f"d1{tag}.db",
                               [("a", [(1, 5, 4), (2, None, 3)])])
        d1x = self.make_dataset(f"d1x{tag}.db", d1_alt_shards) \
            if d1_alt_shards else d1
        d2 = self.make_dataset(f"d2{tag}.db",
                               [("a", [(1, 5, 4), (2, None, 3)])])
        l1 = self.build(d0, d1, [], f"l1{tag}.db")
        l2 = self.build(d1x, d2, [], f"l2{tag}.db")
        return l1, l2

    def test_consistent_chain_ok(self):
        l1, l2 = self.make_chain()
        req = self.write_requests([req_json("a", 1, 0, 4)])
        _summary, lines = self.run_report([l1, l2], 2, req)
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0]["periods"], 3)
        self.assertFalse(lines[0]["exact"])
        self.assertEqual(lines[0]["verdict"], "符合")

    def check_mismatch(self, alt_shards, tag):
        l1, l2 = self.make_chain(alt_shards, tag)
        req = self.write_requests([req_json("a", 1, 0, 1)], f"req{tag}.jsonl")
        out = self.path(f"out{tag}.jsonl")
        with self.assertRaises(ResidencyError) as ctx:
            residency_report([l1, l2], 1, req, out)
        self.assertIn("第 1 期", str(ctx.exception))
        self.assertIn("共用期", str(ctx.exception))
        self.assertFalse(out.exists())
        self.assertEqual(self.tmp_files(), [])

    def test_weight_mismatch(self):
        self.check_mismatch([("a", [(1, 5, 5), (2, None, 3)])], "w")

    def test_unknown_only_matches_unknown(self):
        self.check_mismatch([("a", [(1, 5, 4), (2, 7, 3)])], "u")

    def test_equal_totals_not_enough(self):
        # 总量都是 7，但记录登记不同：总量相等不能代替逐条一致
        self.check_mismatch([("a", [(1, 5, 7)])], "t")

    def test_missing_record(self):
        self.check_mismatch([("a", [(2, None, 3)])], "m")

    def test_inconsistency_at_later_shared_period(self):
        d0 = self.make_dataset("e0.db", [("a", [(1, 5, 2)])])
        d1 = self.make_dataset("e1.db", [("a", [(1, 5, 2)])])
        d2 = self.make_dataset("e2.db", [("a", [(1, 5, 2)])])
        d2x = self.make_dataset("e2x.db", [("a", [(1, 6, 2)])])  # 估值不同
        d3 = self.make_dataset("e3.db", [("a", [(1, 5, 2)])])
        l1 = self.build(d0, d1, [], "k1.db")
        l2 = self.build(d1, d2, [], "k2.db")
        l3 = self.build(d2x, d3, [], "k3.db")
        req = self.write_requests([req_json("a", 1, 0, 1)])
        with self.assertRaises(ResidencyError) as ctx:
            residency_report([l1, l2, l3], 1, req, self.path("o.jsonl"))
        self.assertIn("第 2 期", str(ctx.exception))
        self.assertEqual(self.tmp_files(), [])


# ------------------------------------------------------------ 单份台账

class SingleLedgerTest(ResidencyCase):
    def test_carry_gives_lower_bound(self):
        d0 = self.make_dataset("d0.db", [("a", [(1, 5, 5)])])
        d1 = self.make_dataset("d1.db", [("a", [(1, 9, 5)])])  # 估值变了仍承接
        l1 = self.build(d0, d1, [], "l1.db")
        req = self.write_requests([req_json("a", 1, 0, 5)])
        _s, lines = self.run_report([l1], 2, req)
        self.assertEqual(lines, [{
            "line_no": 1, "sender": "a", "record_no": 1,
            "start": "0", "length": "5", "verdict": "符合",
            "periods": 2, "exact": False,
            "basis": {"period": 0, "sender": "a", "record_no": 1,
                      "start": "0"}}])
        # 门槛超过窗口能确认的下限：历史不足
        _s, lines = self.run_report([l1], 3, req, "r2.jsonl")
        self.assertEqual(lines[0]["verdict"], "历史不足")
        self.assertEqual(lines[0]["periods"], 2)
        self.assertFalse(lines[0]["exact"])

    def test_new_entry_is_exact(self):
        d0 = self.make_dataset("d0.db", [("a", [(1, 5, 2)])])
        d1 = self.make_dataset("d1.db", [("a", [(1, 5, 2), (2, 7, 3)])])
        l1 = self.build(d0, d1, [], "l1.db")
        new_id = self.entry_ids(l1, "new")[0]
        req = self.write_requests([req_json("a", 2, 0, 3)])
        _s, lines = self.run_report([l1], 1, req)
        self.assertEqual(lines, [{
            "line_no": 1, "sender": "a", "record_no": 2,
            "start": "0", "length": "3", "verdict": "符合",
            "periods": 1, "exact": True,
            "basis": {"period": 1, "sender": "a", "record_no": 2,
                      "start": "0", "entry_id": new_id}}])
        _s, lines = self.run_report([l1], 2, req, "r2.jsonl")
        self.assertEqual(lines[0]["verdict"], "不符合")
        self.assertTrue(lines[0]["exact"])

    def test_foreign_transfer_starts_clock(self):
        d0 = self.make_dataset("d0.db", [("w", [(1, 5, 5)])])
        d1 = self.make_dataset("d1.db", [("e", [(1, 5, 5)])])
        l1 = self.build(d0, d1, [decl_json(("w", 1, 0), ("e", 1, 0), 5)],
                        "l1.db")
        tr_id = self.entry_ids(l1, "transfer")[0]
        req = self.write_requests([req_json("e", 1, 0, 5)])
        _s, lines = self.run_report([l1], 1, req)
        self.assertEqual(lines, [{
            "line_no": 1, "sender": "e", "record_no": 1,
            "start": "0", "length": "5", "verdict": "符合",
            "periods": 1, "exact": True,
            "basis": {"period": 1, "sender": "e", "record_no": 1,
                      "start": "0", "entry_id": tr_id}}])
        _s, lines = self.run_report([l1], 2, req, "r2.jsonl")
        self.assertEqual(lines[0]["verdict"], "不符合")

    def test_same_region_transfer_continues(self):
        # 换号 + 偏移变化 + 估值变化都不重新计时
        d0 = self.make_dataset("d0.db", [("a", [(1, 5, 6), (2, 7, 4)])])
        d1 = self.make_dataset("d1.db", [("a", [(9, 9, 10)])])
        l1 = self.build(d0, d1, [
            decl_json(("a", 2, 1), ("a", 9, 0), 2),
            decl_json(("a", 1, 2), ("a", 9, 3), 4),
        ], "l1.db")
        new_ids = self.entry_ids(l1, "new")
        req = self.write_requests([req_json("a", 9, 0, 10)])
        _s, lines = self.run_report([l1], 2, req)
        self.assertEqual([(l["start"], l["length"], l["verdict"],
                           l["periods"], l["exact"]) for l in lines], [
            ("0", "2", "符合", 2, False),    # 来自第 0 期 a2[1,3)
            ("2", "1", "不符合", 1, True),   # 第 1 期新增
            ("3", "4", "符合", 2, False),    # 来自第 0 期 a1[2,6)
            ("7", "3", "不符合", 1, True),   # 第 1 期新增
        ])
        self.assertEqual(lines[0]["basis"], {"period": 0, "sender": "a",
                                             "record_no": 2, "start": "1"})
        self.assertEqual(lines[2]["basis"], {"period": 0, "sender": "a",
                                             "record_no": 1, "start": "2"})
        self.assertEqual(lines[1]["basis"]["entry_id"], new_ids[0])
        self.assertEqual(lines[3]["basis"]["entry_id"], new_ids[-1])


# ------------------------------------------------------------ 多期拆分（示例场景）

class SplitVerdictTest(ResidencyCase):
    """同一条请求因中途转入与持续留存被拆成不同结论。"""

    def build_demo(self):
        d0 = self.make_dataset("d0.db", [("east", [(5, 3, 4)])])
        d1 = self.make_dataset("d1.db", [("east", [(1, 3, 4)]),
                                         ("west", [(2, 8, 6)])])
        d2 = self.make_dataset("d2.db", [("east", [(1, 3, 10)])])
        l1 = self.build(d0, d1, [decl_json(("east", 5, 0), ("east", 1, 0), 4)],
                        "l1.db")
        l2 = self.build(d1, d2,
                        [decl_json(("west", 2, 0), ("east", 1, 4), 6)],
                        "l2.db")
        return [l1, l2]

    def test_request_splits(self):
        ledgers = self.build_demo()
        tr_id = self.entry_ids(ledgers[1], "transfer")[0]
        req = self.write_requests([req_json("east", 1, 0, 10)])
        summary, lines = self.run_report(ledgers, 3, req)
        self.assertEqual(lines, [
            {"line_no": 1, "sender": "east", "record_no": 1,
             "start": "0", "length": "4", "verdict": "符合",
             "periods": 3, "exact": False,
             "basis": {"period": 0, "sender": "east", "record_no": 5,
                       "start": "0"}},
            {"line_no": 1, "sender": "east", "record_no": 1,
             "start": "4", "length": "6", "verdict": "不符合",
             "periods": 1, "exact": True,
             "basis": {"period": 2, "sender": "east", "record_no": 1,
                       "start": "4", "entry_id": tr_id}},
        ])
        self.assertEqual(summary["verdicts"], {"符合": 1, "不符合": 1,
                                               "历史不足": 0})
        # 门槛降到 1：两段都符合，但依据与期数不变
        _s, lines = self.run_report(ledgers, 1, req, "r2.jsonl")
        self.assertEqual([l["verdict"] for l in lines], ["符合", "符合"])
        self.assertEqual([l["periods"] for l in lines], [3, 1])
        self.assertEqual([l["exact"] for l in lines], [False, True])


class ExitReaddTest(ResidencyCase):
    def test_exit_then_readd_same_record_no_inherits_nothing(self):
        d0 = self.make_dataset("d0.db", [("a", [(1, 5, 5)])])
        d1 = self.make_dataset("d1.db", [("b", [(1, 5, 1)])])  # a1 退出
        d2 = self.make_dataset("d2.db", [("a", [(1, 5, 5)])])  # 同号再新增
        l1 = self.build(d0, d1, [], "l1.db")
        l2 = self.build(d1, d2, [], "l2.db")
        new_id = self.entry_ids(l2, "new")[0]
        req = self.write_requests([req_json("a", 1, 0, 5)])
        _s, lines = self.run_report([l1, l2], 2, req)
        self.assertEqual(lines, [{
            "line_no": 1, "sender": "a", "record_no": 1,
            "start": "0", "length": "5", "verdict": "不符合",
            "periods": 1, "exact": True,
            "basis": {"period": 2, "sender": "a", "record_no": 1,
                      "start": "0", "entry_id": new_id}}])


# ------------------------------------------------------------ 合并规则

class MergeTest(ResidencyCase):
    def test_merge_continuous_evidence(self):
        # 两段经不同明细追到第 0 期同记录且副本位连续：合为一段
        d0 = self.make_dataset("d0.db", [("a", [(7, 1, 10)])])
        d1 = self.make_dataset("d1.db", [("a", [(8, 1, 10)])])
        d2 = self.make_dataset("d2.db", [("a", [(9, 1, 10)])])
        l1 = self.build(d0, d1, [decl_json(("a", 7, 0), ("a", 8, 0), 10)],
                        "l1.db")
        l2 = self.build(d1, d2, [decl_json(("a", 8, 0), ("a", 9, 0), 5),
                                 decl_json(("a", 8, 5), ("a", 9, 5), 5)],
                        "l2.db")
        req = self.write_requests([req_json("a", 9, 0, 10)])
        _s, lines = self.run_report([l1, l2], 3, req)
        self.assertEqual(lines, [{
            "line_no": 1, "sender": "a", "record_no": 9,
            "start": "0", "length": "10", "verdict": "符合",
            "periods": 3, "exact": False,
            "basis": {"period": 0, "sender": "a", "record_no": 7,
                      "start": "0"}}])

    def test_no_merge_when_evidence_jumps(self):
        # 依据端副本位不连续：分开
        d0 = self.make_dataset("d0.db", [("a", [(7, 1, 60)])])
        d1 = self.make_dataset("d1.db", [("a", [(8, 1, 10)])])
        d2 = self.make_dataset("d2.db", [("a", [(9, 1, 10)])])
        l1 = self.build(d0, d1, [decl_json(("a", 7, 0), ("a", 8, 0), 5),
                                 decl_json(("a", 7, 50), ("a", 8, 5), 5)],
                        "l1.db")
        l2 = self.build(d1, d2, [decl_json(("a", 8, 0), ("a", 9, 0), 5),
                                 decl_json(("a", 8, 5), ("a", 9, 5), 5)],
                        "l2.db")
        req = self.write_requests([req_json("a", 9, 0, 10)])
        _s, lines = self.run_report([l1, l2], 3, req)
        self.assertEqual([(l["start"], l["length"], l["basis"]["start"])
                          for l in lines],
                         [("0", "5", "0"), ("5", "5", "50")])

    def test_merge_same_entry_across_parents(self):
        # 两段经不同转列落到上一期同一条新增明细：合并，依据含 entry_id
        d0 = self.make_dataset("d0.db", [("a", [(1, 1, 3)])])
        d1 = self.make_dataset("d1.db", [("a", [(8, 1, 10)])])  # a8 整段新增
        d2 = self.make_dataset("d2.db", [("a", [(9, 1, 10)])])
        l1 = self.build(d0, d1, [], "l1.db")
        l2 = self.build(d1, d2, [decl_json(("a", 8, 0), ("a", 9, 0), 5),
                                 decl_json(("a", 8, 5), ("a", 9, 5), 5)],
                        "l2.db")
        new_id = self.entry_ids(l1, "new")[0]
        req = self.write_requests([req_json("a", 9, 0, 10)])
        _s, lines = self.run_report([l1, l2], 2, req)
        self.assertEqual(lines, [{
            "line_no": 1, "sender": "a", "record_no": 9,
            "start": "0", "length": "10", "verdict": "符合",
            "periods": 2, "exact": True,
            "basis": {"period": 1, "sender": "a", "record_no": 8,
                      "start": "0", "entry_id": new_id}}])

    def test_no_merge_different_entries(self):
        d0 = self.make_dataset("d0.db", [("w", [(1, 1, 5), (2, 1, 5)])])
        d1 = self.make_dataset("d1.db", [("e", [(9, 1, 10)])])
        l1 = self.build(d0, d1, [decl_json(("w", 1, 0), ("e", 9, 0), 5),
                                 decl_json(("w", 2, 0), ("e", 9, 5), 5)],
                        "l1.db")
        tr_ids = self.entry_ids(l1, "transfer")
        req = self.write_requests([req_json("e", 9, 0, 10)])
        _s, lines = self.run_report([l1], 1, req)
        self.assertEqual(len(lines), 2)
        self.assertEqual([l["basis"]["entry_id"] for l in lines], tr_ids)
        self.assertEqual([l["basis"]["record_no"] for l in lines], [9, 9])


# ------------------------------------------------------------ 请求处理

class RequestsHandlingTest(ResidencyCase):
    def make_one(self):
        d0 = self.make_dataset("d0.db", [("a", [(1, 5, 10)]),
                                         ("b", [(1, 5, 2)])])
        d1 = self.make_dataset("d1.db", [("a", [(1, 5, 10)]),
                                         ("b", [(1, 5, 2)])])
        return self.build(d0, d1, [], "l1.db")

    def test_blank_lines_count_in_line_numbers(self):
        l1 = self.make_one()
        req = self.write_requests(["", req_json("a", 1, 0, 4), "",
                                   req_json("b", 1, 0, 2)])
        _s, lines = self.run_report([l1], 1, req)
        self.assertEqual([l["line_no"] for l in lines], [2, 4])

    def test_duplicate_and_overlapping_requests(self):
        l1 = self.make_one()
        req = self.write_requests([req_json("a", 1, 0, 6),
                                   req_json("a", 1, 0, 6),   # 重复
                                   req_json("a", 1, 2, 4)])  # 相交
        summary, lines = self.run_report([l1], 1, req)
        self.assertEqual(summary["requests"], 3)
        self.assertEqual([l["line_no"] for l in lines], [1, 2, 3])
        self.assertEqual((lines[0]["start"], lines[0]["length"]), ("0", "6"))
        self.assertEqual((lines[2]["start"], lines[2]["length"]), ("2", "4"))
        self.assertEqual(lines[0]["verdict"], lines[1]["verdict"])

    def test_output_order_by_line_then_start(self):
        l1 = self.make_one()
        req = self.write_requests([req_json("b", 1, 0, 2),
                                   req_json("a", 1, 6, 4),
                                   req_json("a", 1, 0, 3)])
        _s, lines = self.run_report([l1], 1, req)
        keys = [(l["line_no"], int(l["start"])) for l in lines]
        self.assertEqual(keys, sorted(keys))
        self.assertEqual([l["line_no"] for l in lines], [1, 2, 3])

    def test_bad_lines(self):
        l1 = self.make_one()
        cases = [
            "not json",
            "[1, 2]",
            json.dumps({"record_no": 1, "start": "0", "length": "1"}),
            json.dumps({"sender": "a", "start": "0", "length": "1"}),
            json.dumps({"sender": "a", "record_no": 1, "length": "1"}),
            json.dumps({"sender": "a", "record_no": 1, "start": "0"}),
            req_json("a", 1, 0, 0),      # 长度非正
            req_json("a", 1, 0, -1),     # 非十进制
            json.dumps({"sender": "a", "record_no": 1, "start": 0,
                        "length": "1"}),                    # start 非字符串
            json.dumps({"sender": "a", "record_no": "1", "start": "0",
                        "length": "1"}),                    # record_no 非整数
            json.dumps({"sender": "", "record_no": 1, "start": "0",
                        "length": "1"}),                    # sender 为空
            json.dumps({"sender": "a", "record_no": True, "start": "0",
                        "length": "1"}),                    # record_no 为布尔
            json.dumps({"sender": "a", "record_no": 1, "start": "-1",
                        "length": "1"}),                    # 起点为负
        ]
        for i, bad in enumerate(cases):
            with self.subTest(case=i):
                req = self.write_requests([bad], f"bad{i}.jsonl")
                out = self.path(f"bad{i}.out")
                with self.assertRaises(ResidencyError) as ctx:
                    residency_report([l1], 1, req, out)
                self.assertIn("第 1 行", str(ctx.exception))
                self.assertFalse(out.exists())
        self.assertEqual(self.tmp_files(), [])

    def test_record_not_found(self):
        l1 = self.make_one()
        req = self.write_requests([req_json("a", 99, 0, 1)])
        out = self.path("o.jsonl")
        with self.assertRaises(ResidencyError) as ctx:
            residency_report([l1], 1, req, out)
        self.assertIn("第 1 行", str(ctx.exception))
        self.assertIn("不存在", str(ctx.exception))
        self.assertFalse(out.exists())
        self.assertEqual(self.tmp_files(), [])

    def test_interval_out_of_bounds(self):
        l1 = self.make_one()
        req = self.write_requests([req_json("a", 1, 8, 3)])  # 8+3 > 10
        out = self.path("o.jsonl")
        with self.assertRaises(ResidencyError) as ctx:
            residency_report([l1], 1, req, out)
        self.assertIn("第 1 行", str(ctx.exception))
        self.assertIn("越过", str(ctx.exception))
        self.assertFalse(out.exists())
        # 边界：start + length == 权重 合法
        req2 = self.write_requests([req_json("a", 1, 9, 1)], "r2.jsonl")
        _s, lines = self.run_report([l1], 1, req2, "o2.jsonl")
        self.assertEqual(len(lines), 1)
        self.assertEqual((lines[0]["start"], lines[0]["length"]), ("9", "1"))

    def test_empty_requests_delivers_empty_file(self):
        l1 = self.make_one()
        req = self.write_requests(["", "  "])
        summary, lines = self.run_report([l1], 1, req)
        self.assertEqual(lines, [])
        self.assertEqual(summary["requests"], 0)
        self.assertEqual(summary["segments"], 0)
        self.assertEqual(summary["verdicts"],
                         {"符合": 0, "不符合": 0, "历史不足": 0})


# ------------------------------------------------------------ 交付语义

class DeliveryTest(ResidencyCase):
    def make_one(self):
        d0 = self.make_dataset("d0.db", [("a", [(1, 5, 10)])])
        d1 = self.make_dataset("d1.db", [("a", [(1, 5, 10)])])
        return self.build(d0, d1, [], "l1.db")

    def test_existing_target_not_overwritten(self):
        l1 = self.make_one()
        out = self.path("keep.jsonl")
        out.write_bytes(b"keep me")
        req = self.write_requests([req_json("a", 1, 0, 1)])
        with self.assertRaises(ResidencyError):
            residency_report([l1], 1, req, out)
        self.assertEqual(out.read_bytes(), b"keep me")
        self.assertEqual(self.tmp_files(), [])

    def test_invalid_inputs(self):
        l1 = self.make_one()
        req = self.write_requests([req_json("a", 1, 0, 1)])
        with self.assertRaises(ResidencyError):
            residency_report([], 1, req, self.path("o.jsonl"))
        with self.assertRaises(ResidencyError):
            residency_report([self.path("nope.db")], 1, req,
                             self.path("o.jsonl"))
        with self.assertRaises(ResidencyError):  # 数据集不是台账
            residency_report([self.path("d0.db")], 1, req, self.path("o.jsonl"))
        for bad in (0, -1, "2", 2.5, True, None):
            with self.subTest(min_periods=bad):
                with self.assertRaises(ResidencyError):
                    residency_report([l1], bad, req, self.path("o.jsonl"))
        with self.assertRaises(ResidencyError):
            residency_report([l1], 1, self.path("nope.jsonl"),
                             self.path("o.jsonl"))
        with self.assertRaises(ResidencyError):
            residency_report([l1], 1, req, self.path("no_dir/o.jsonl"))
        self.assertEqual(self.tmp_files(), [])

    def test_single_ledger_path_accepted(self):
        l1 = self.make_one()
        req = self.write_requests([req_json("a", 1, 0, 2)])
        summary, lines = self.run_report(str(l1), 1, req)  # 单份可给字符串
        self.assertEqual(summary["ledgers"], 1)
        self.assertEqual(len(lines), 1)

    def test_ledgers_untouched(self):
        d0 = self.make_dataset("d0.db", [("a", [(1, 5, 6), (2, 7, 4)])])
        d1 = self.make_dataset("d1.db", [("a", [(9, 9, 10)])])
        l1 = self.build(d0, d1, [decl_json(("a", 1, 0), ("a", 9, 0), 6)],
                        "l1.db")
        digest = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
        before = digest(l1)
        req = self.write_requests([req_json("a", 9, 0, 10)])
        self.run_report([l1], 1, req)
        self.assertEqual(before, digest(l1))
        with Ledger(l1) as led:  # 台账仍可独立核对
            led.verify()


# ------------------------------------------------------------ 大副本位

class BigPositionsTest(ResidencyCase):
    def test_big_positions_exact(self):
        BIG = 2 ** 63 + 20
        d0 = self.make_dataset("d0.db", [("m", [(1, 10, str(BIG))])])
        d1 = self.make_dataset("d1.db", [("m", [(1, 10, str(BIG))]),
                                         ("x", [(1, 1, 10)])])
        d2 = self.make_dataset("d2.db", [("m", [(1, 10, str(BIG))])])
        l1 = self.build(d0, d1, [], "l1.db")
        l2 = self.build(d1, d2,
                        [decl_json(("x", 1, 0), ("m", 1, 2 ** 63 + 10), 10)],
                        "l2.db")
        req = self.write_requests([req_json("m", 1, 2 ** 63 + 5, 15)])
        _s, lines = self.run_report([l1, l2], 3, req)
        self.assertEqual(len(lines), 2)
        self.assertEqual((lines[0]["start"], lines[0]["length"]),
                         (str(2 ** 63 + 5), "5"))
        self.assertEqual(lines[0]["verdict"], "符合")
        self.assertEqual(lines[0]["basis"]["start"], str(2 ** 63 + 5))
        self.assertNotIn("entry_id", lines[0]["basis"])
        self.assertEqual((lines[1]["start"], lines[1]["length"]),
                         (str(2 ** 63 + 10), "10"))
        self.assertEqual(lines[1]["verdict"], "不符合")
        self.assertEqual(lines[1]["basis"]["start"], str(2 ** 63 + 10))
        self.assertIn("entry_id", lines[1]["basis"])


# ------------------------------------------------------------ 热点记录

class HotRecordTest(ResidencyCase):
    def test_hot_record_merges_to_one_segment(self):
        W, CH = 9000, 3
        n = W // CH
        d0 = self.make_dataset("d0.db", [("h", [(1, 1, W)])])
        d1 = self.make_dataset("d1.db", [("h", [(2, 1, W)])])
        d2 = self.make_dataset("d2.db", [("h", [(3, 1, W)])])
        m1 = [decl_json(("h", 1, CH * i), ("h", 2, CH * i), CH)
              for i in range(n)]
        m2 = [decl_json(("h", 2, CH * i), ("h", 3, CH * i), CH)
              for i in range(n)]
        l1 = self.build(d0, d1, m1, "l1.db")
        l2 = self.build(d1, d2, m2, "l2.db")
        req = self.write_requests([req_json("h", 3, 0, W)])
        summary, lines = self.run_report([l1, l2], 3, req)
        # 3000 条同地区转列 × 2 期，全部追到第 0 期且连续：合为一段
        self.assertEqual(lines, [{
            "line_no": 1, "sender": "h", "record_no": 3,
            "start": "0", "length": str(W), "verdict": "符合",
            "periods": 3, "exact": False,
            "basis": {"period": 0, "sender": "h", "record_no": 1,
                      "start": "0"}}])
        self.assertEqual(summary["segments"], 1)

    def test_hot_record_alternating_origins(self):
        n, CH = 2000, 2
        half = (n // 2) * CH
        d0 = self.make_dataset("d0.db", [("h", [(2, 1, half)]),
                                         ("w", [(1, 1, half)])])
        d1 = self.make_dataset("d1.db", [("h", [(2, 1, half)]),
                                         ("w", [(1, 1, half)])])
        d2 = self.make_dataset("d2.db", [("h", [(3, 1, n * CH)])])
        l1 = self.build(d0, d1, [], "l1.db")  # 全部默认承接
        m2 = []
        hi = wi = 0
        for i in range(n):
            if i % 2 == 0:  # 同地区转列
                m2.append(decl_json(("h", 2, hi), ("h", 3, CH * i), CH))
                hi += CH
            else:           # 跨地区转列
                m2.append(decl_json(("w", 1, wi), ("h", 3, CH * i), CH))
                wi += CH
        l2 = self.build(d1, d2, m2, "l2.db")
        req = self.write_requests([req_json("h", 3, 0, n * CH)])
        summary, lines = self.run_report([l1, l2], 3, req)
        self.assertEqual(len(lines), n)
        for i, ln in enumerate(lines):
            self.assertEqual((ln["start"], ln["length"]),
                             (str(CH * i), str(CH)))
            if i % 2 == 0:
                self.assertEqual(ln["verdict"], "符合")
                self.assertEqual(ln["basis"]["period"], 0)
                self.assertEqual(ln["basis"]["record_no"], 2)
            else:
                self.assertEqual(ln["verdict"], "不符合")
                self.assertEqual(ln["periods"], 1)
                self.assertEqual(ln["basis"]["period"], 2)
        self.assertEqual(summary["verdicts"]["符合"], n // 2)
        self.assertEqual(summary["verdicts"]["不符合"], n // 2)


# ------------------------------------------------------------ 随机交叉核对

class RandomizedCrossCheckTest(ResidencyCase):
    """随机多期链 + 随机请求：与逐副本展开参照模型全量对照。"""

    def random_decls(self, rng, old_rows, new_rows):
        old_used, new_used, decls = {}, {}, []
        if not old_rows or not new_rows:
            return decls
        for _ in range(rng.randint(0, 30)):
            okey = rng.choice(list(old_rows))
            nkey = rng.choice(list(new_rows))
            ow, nw = old_rows[okey][1], new_rows[nkey][1]
            length = rng.randint(1, 4)
            if length > ow or length > nw:
                continue
            for _try in range(20):
                os_ = rng.randint(0, ow - length)
                ns_ = rng.randint(0, nw - length)
                ospan = set(range(os_, os_ + length))
                nspan = set(range(ns_, ns_ + length))
                if not (ospan & old_used.get(okey, set())) \
                        and not (nspan & new_used.get(nkey, set())):
                    old_used.setdefault(okey, set()).update(ospan)
                    new_used.setdefault(nkey, set()).update(nspan)
                    decls.append(decl_json((okey[0], okey[1], os_),
                                           (nkey[0], nkey[1], ns_), length))
                    break
        return decls

    def random_case(self, rng):
        senders = [f"r{i:02d}" for i in range(rng.randint(2, 3))]
        n_periods = rng.randint(2, 5)
        rows = {}
        for s in senders:
            for r in rng.sample(range(1, 25), rng.randint(1, 4)):
                rows[(s, r)] = (rng.choice([None, -5, 0, 3, 7, 100]),
                                rng.randint(1, 10))
        periods = [rows]
        transitions = []
        for _t in range(1, n_periods):
            old_rows = periods[-1]
            new_rows = {}
            for (s, r), (v, w) in old_rows.items():
                if rng.random() < 0.75:  # 同号留存（权重/估值可变）
                    new_rows[(s, r)] = (rng.choice([v, None, 3, 7]),
                                        rng.randint(1, 10))
            for _ in range(rng.randint(0, 3)):  # 新增（可能复用旧期记录号）
                s = rng.choice(senders)
                r = rng.randint(1, 25)
                if (s, r) not in new_rows:
                    new_rows[(s, r)] = (rng.choice([None, -5, 0, 3, 7, 100]),
                                        rng.randint(1, 10))
            transitions.append(self.random_decls(rng, old_rows, new_rows))
            periods.append(new_rows)
        return periods, transitions

    def check_case(self, seed):
        rng = random.Random(seed)
        periods, transitions = self.random_case(rng)
        datasets = [self.make_dataset(f"d{seed}_{i}.db", rows_to_shards(rows))
                    for i, rows in enumerate(periods)]
        ledgers = []
        for i, decls in enumerate(transitions):
            ledgers.append(self.build(datasets[i], datasets[i + 1], decls,
                                      f"l{seed}_{i}.db"))
        # 随机请求（含空行、重复与相交），记录期望行号
        req_lines, req_specs = [], []
        final_rows = periods[-1]
        if final_rows:
            keys = sorted(final_rows)
            for _ in range(rng.randint(1, 8)):
                if rng.random() < 0.2:
                    req_lines.append("")  # 空行占行号
                s, r = keys[rng.randrange(len(keys))]
                w = final_rows[(s, r)][1]
                start = rng.randint(0, w - 1)
                length = rng.randint(1, w - start)
                req_lines.append(req_json(s, r, start, length))
                req_specs.append((len(req_lines), s, r, start, length))
            if req_specs:
                first = req_specs[0]
                req_lines.append(req_json(*first[1:]))  # 重复第一条
                req_specs.append((len(req_lines), *first[1:]))
        min_periods = rng.randint(1, 6)
        req_path = self.write_requests(req_lines, f"req{seed}.jsonl")
        summary, lines = self.run_report(ledgers, min_periods, req_path,
                                         f"rep{seed}.jsonl")
        model = ResidencyModel(ledgers)
        expected = []
        for line_no, s, r, start, length in req_specs:
            expected.extend(
                model.expected(line_no, s, r, start, length, min_periods))
        self.assertEqual(lines, expected)
        self.assertEqual(summary["requests"], len(req_specs))
        self.assertEqual(summary["segments"], len(expected))
        cnt = {"符合": 0, "不符合": 0, "历史不足": 0}
        for e in expected:
            cnt[e["verdict"]] += 1
        self.assertEqual(summary["verdicts"], cnt)

    def test_seeds(self):
        for seed in (1, 2, 3, 4, 5, 6):
            with self.subTest(seed=seed):
                self.check_case(seed)


# ------------------------------------------------------------ 示例可复跑

class ExampleTest(unittest.TestCase):
    def test_example_runs(self):
        root = Path(__file__).resolve().parents[1]
        script = root / "examples" / "residency_example.py"
        proc = subprocess.run([sys.executable, str(script)],
                              capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn('"verdict": "符合"', proc.stdout)
        self.assertIn('"verdict": "不符合"', proc.stdout)


if __name__ == "__main__":
    unittest.main()
