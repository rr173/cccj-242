# -*- coding: utf-8 -*-
"""迁移台账的测试。

包含一个独立的逐副本展开参照模型：把两期记录与转列清单展开到副本位，
计算期望的转列/承接/退出/新增区间与地区合计，与台账交付文件交叉核对。
"""

import hashlib
import json
import os
import random
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ordered_exchange import (Dataset, Ledger, LedgerError, build_ledger)


# ------------------------------------------------------------ 参照模型

class LedgerModel:
    """逐副本展开的独立参照（仅适用小权重）。

    old_rows/new_rows: {(sender, record_no): (value, weight)}
    decls: [(line_no, (o_sender, o_rec, o_start), (n_sender, n_rec, n_start),
             length)]
    """

    def __init__(self, old_rows, new_rows, decls):
        self.old_rows = old_rows
        self.new_rows = new_rows
        self.decls = decls
        self.old_used = {}
        self.new_used = {}
        for _ln, o, n, length in decls:
            for i in range(length):
                self.old_used.setdefault((o[0], o[1]), set()).add(o[2] + i)
                self.new_used.setdefault((n[0], n[1]), set()).add(n[2] + i)

    @staticmethod
    def _free(rows, used, key):
        if key not in rows:
            return set()
        return set(range(rows[key][1])) - used.get(key, set())

    @staticmethod
    def _compress(positions):
        out = []
        for p in sorted(positions):
            if out and out[-1][0] + out[-1][1] == p:
                out[-1] = (out[-1][0], out[-1][1] + 1)
            else:
                out.append((p, 1))
        return out

    def entries(self):
        """期望明细（与 ledger_tuples 同形的排序元组列表）。"""
        out = []
        for ln, o, n, length in self.decls:
            out.append(("transfer", (o[0], o[1]), o[2],
                        (n[0], n[1]), n[2], length, ln))
        for key in set(self.old_rows) | set(self.new_rows):
            ofree = self._free(self.old_rows, self.old_used, key)
            nfree = self._free(self.new_rows, self.new_used, key)
            for s, l in self._compress(ofree & nfree):
                out.append(("carry", key, s, l))
            for s, l in self._compress(ofree - nfree):
                out.append(("exit", key, s, l))
            for s, l in self._compress(nfree - ofree):
                out.append(("new", key, s, l))
        return sorted(out)

    def region_totals(self):
        agg = {}
        for e in self.entries():
            if e[0] == "transfer":
                key, length = (e[1][0], e[3][0]), e[5]
            elif e[0] == "carry":
                key, length = (e[1][0], e[1][0]), e[3]
            elif e[0] == "exit":
                key, length = (e[1][0], None), e[3]
            else:
                key, length = (None, e[1][0]), e[3]
            agg[key] = agg.get(key, 0) + length
        return agg


def ledger_tuples(led):
    """台账明细归一成与 LedgerModel.entries 同形的元组。"""
    out = []
    for e in led.entries():
        kind = e["kind"]
        if kind == "transfer":
            out.append(("transfer",
                        (e["old"]["sender"], e["old"]["record_no"]),
                        e["old"]["start"],
                        (e["new"]["sender"], e["new"]["record_no"]),
                        e["new"]["start"], e["length"], e["line_no"]))
        elif kind == "carry":
            out.append(("carry",
                        (e["old"]["sender"], e["old"]["record_no"]),
                        e["old"]["start"], e["length"]))
        elif kind == "exit":
            out.append(("exit",
                        (e["old"]["sender"], e["old"]["record_no"]),
                        e["old"]["start"], e["length"]))
        else:
            out.append(("new",
                        (e["new"]["sender"], e["new"]["record_no"]),
                        e["new"]["start"], e["length"]))
    return sorted(out)


def rows_to_shards(rows):
    """{(sender, rec): (value, weight)} -> Dataset.create 可用的分片列表。"""
    by_sender = {}
    for (s, r), (v, w) in rows.items():
        by_sender.setdefault(s, []).append((r, v, w))
    shards = []
    for s in sorted(by_sender):
        rs = sorted(by_sender[s],
                    key=lambda t: (t[1] is None, t[1] if t[1] is not None else 0))
        shards.append((s, rs))
    return shards


def decl_json(o, n, length):
    return json.dumps({
        "old": {"sender": o[0], "record_no": o[1], "start": str(o[2])},
        "new": {"sender": n[0], "record_no": n[1], "start": str(n[2])},
        "length": str(length)}, ensure_ascii=False)


def manifest_lines(decls):
    return [decl_json(o, n, length) for _ln, o, n, length in decls]


# ------------------------------------------------------------ 测试基类

class LedgerTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="oled_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def path(self, name):
        return self.tmp / name

    def make_dataset(self, name, shards):
        path = self.path(name)
        ds = Dataset.create(path, shards)
        ds.close()
        return path

    def write_manifest(self, lines, name="manifest.jsonl"):
        p = self.path(name)
        with open(p, "w", encoding="utf-8") as fh:
            for ln in lines:
                fh.write(ln if ln.endswith("\n") else ln + "\n")
        return p

    def tmp_files(self):
        return [f for f in os.listdir(self.tmp) if f.endswith(".tmp")]

    def build(self, old_path, new_path, manifest_path, out_name="ledger.db"):
        out = self.path(out_name)
        summary = build_ledger(old_path, new_path, manifest_path, out)
        self.assertEqual(self.tmp_files(), [])  # 不留临时文件
        return out, summary


# ------------------------------------------------------------ 小样例

class CarryHoleTest(LedgerTestCase):
    """两端占用不同偏移时的承接孔洞：余量不压紧再配。"""

    def test_offset_claims_leave_holes(self):
        old = self.make_dataset("old.db", [
            ("a", [(1, 5, 10)]),
            ("b", [(1, 7, 4)]),
        ])
        new = self.make_dataset("new.db", [
            ("a", [(1, 5, 10)]),
            ("b", [(1, 7, 6)]),
        ])
        decls = [
            (1, ("a", 1, 2), ("b", 1, 0), 2),  # 旧 a1[2,4) -> 新 b1[0,2)
            (2, ("b", 1, 2), ("a", 1, 7), 2),  # 旧 b1[2,4) -> 新 a1[7,9)
        ]
        manifest = self.write_manifest(manifest_lines(decls))
        out, summary = self.build(old, new, manifest)

        # 旧 a1 空闲 [0,2)∪[4,10)，新 a1 空闲 [0,7)∪[9,10)
        # -> 承接 [0,2)∪[4,7)∪[9,10)，退出 [7,9)，新增 [2,4)
        # 旧 b1 空闲 [0,2)，新 b1 空闲 [2,6) -> 无共同位，全退全增
        expected = sorted([
            ("transfer", ("a", 1), 2, ("b", 1), 0, 2, 1),
            ("transfer", ("b", 1), 2, ("a", 1), 7, 2, 2),
            ("carry", ("a", 1), 0, 2),
            ("carry", ("a", 1), 4, 3),
            ("carry", ("a", 1), 9, 1),
            ("exit", ("a", 1), 7, 2),
            ("exit", ("b", 1), 0, 2),
            ("new", ("a", 1), 2, 2),
            ("new", ("b", 1), 2, 4),
        ])
        with Ledger(out) as led:
            self.assertEqual(ledger_tuples(led), expected)
            # 按旧、新地区组合的精确合计
            rt = {(r["old_sender"], r["new_sender"]): r["weight"]
                  for r in led.region_totals()}
            self.assertEqual(rt, {
                ("a", "a"): 6, ("a", "b"): 2, ("b", "a"): 2,
                ("a", None): 2, ("b", None): 2,
                (None, "a"): 2, (None, "b"): 4,
            })
            # 按侧汇总还原该期各地区总权重
            self.assertEqual(led.sender_totals(0), {"a": 10, "b": 4})
            self.assertEqual(led.sender_totals(1), {"a": 10, "b": 6})
            v = led.verify()
        self.assertEqual(v["kinds"],
                         {"transfer": 2, "carry": 3, "exit": 2, "new": 2})
        self.assertEqual(summary["total_weight"], {"old": 14, "new": 16})


class SplitMergeTest(LedgerTestCase):
    """一笔旧记录拆到几笔新记录；几笔旧记录并进一笔新记录。"""

    def test_split_and_merge(self):
        old = self.make_dataset("old.db", [
            ("s", [(1, 1, 10)]),
            ("t", [(1, 2, 3)]),
            ("u", [(1, 2, 4)]),
        ])
        new = self.make_dataset("new.db", [
            ("x", [(1, 1, 4)]),
            ("y", [(1, 1, 6)]),
            ("z", [(1, 2, 7)]),
        ])
        decls = [
            (1, ("s", 1, 0), ("x", 1, 0), 4),   # 拆分 s1 -> x1 + y1
            (2, ("s", 1, 4), ("y", 1, 0), 6),
            (3, ("t", 1, 0), ("z", 1, 0), 3),   # 合并 t1 + u1 -> z1
            (4, ("u", 1, 0), ("z", 1, 3), 4),
        ]
        manifest = self.write_manifest(manifest_lines(decls))
        out, summary = self.build(old, new, manifest)
        with Ledger(out) as led:
            self.assertEqual(ledger_tuples(led), sorted([
                ("transfer", ("s", 1), 0, ("x", 1), 0, 4, 1),
                ("transfer", ("s", 1), 4, ("y", 1), 0, 6, 2),
                ("transfer", ("t", 1), 0, ("z", 1), 0, 3, 3),
                ("transfer", ("u", 1), 0, ("z", 1), 3, 4, 4),
            ]))
            rt = {(r["old_sender"], r["new_sender"]): r["weight"]
                  for r in led.region_totals()}
            self.assertEqual(rt, {("s", "x"): 4, ("s", "y"): 6,
                                  ("t", "z"): 3, ("u", "z"): 4})
            self.assertEqual(led.sender_totals(0),
                             {"s": 10, "t": 3, "u": 4})
            self.assertEqual(led.sender_totals(1),
                             {"x": 4, "y": 6, "z": 7})
            led.verify()
        self.assertEqual(summary["kinds"]["transfer"], 4)
        self.assertEqual(summary["declarations"], 4)


class ExitNewUnknownTest(LedgerTestCase):
    """退出/新增与"缺端 vs 估值未知"的区分。"""

    def test_exit_new_and_unknown(self):
        old = self.make_dataset("old.db", [
            ("gone", [(2, 5, 2), (1, None, 3)]),
        ])
        new = self.make_dataset("new.db", [
            ("born", [(1, 8, 2), (2, None, 1)]),
        ])
        manifest = self.write_manifest([])
        out, _summary = self.build(old, new, manifest)
        with Ledger(out) as led:
            entries = list(led.entries())
            self.assertEqual(len(entries), 4)
            by_kind = {}
            for e in entries:
                by_kind.setdefault(e["kind"], []).append(e)
            exits = by_kind["exit"]
            self.assertEqual(len(exits), 2)
            # 缺新端：整组为 None；旧端估值未知：is_unknown=True, value=None
            e1 = [e for e in exits if e["old"]["record_no"] == 1][0]
            self.assertIsNone(e1["new"])
            self.assertTrue(e1["old"]["is_unknown"])
            self.assertIsNone(e1["old"]["value"])
            self.assertEqual((e1["old"]["start"], e1["length"]), (0, 3))
            news = by_kind["new"]
            self.assertEqual(len(news), 2)
            n2 = [e for e in news if e["new"]["record_no"] == 2][0]
            self.assertIsNone(n2["old"])
            self.assertTrue(n2["new"]["is_unknown"])
            self.assertIsNone(n2["new"]["value"])
            # 退出缺新地区、新增缺旧地区
            rt = {(r["old_sender"], r["new_sender"]): r["weight"]
                  for r in led.region_totals()}
            self.assertEqual(rt, {("gone", None): 5, (None, "born"): 3})
            led.verify()

    def test_weight_changed_same_record(self):
        # 权重变了、估值变了：同记录同位仍承接，不压紧、不按估值猜测
        old = self.make_dataset("old.db", [("a", [(1, 5, 5)])])
        new = self.make_dataset("new.db", [("a", [(1, 9, 8)])])
        out, _ = self.build(old, new, self.write_manifest([]))
        with Ledger(out) as led:
            self.assertEqual(ledger_tuples(led), sorted([
                ("carry", ("a", 1), 0, 5),
                ("new", ("a", 1), 5, 3),
            ]))
            carry = list(led.entries("carry"))[0]
            self.assertEqual(carry["old"]["value"], 5)
            self.assertEqual(carry["new"]["value"], 9)
            self.assertEqual(carry["old"]["start"], carry["new"]["start"])
            led.verify()
        # 反向：旧长新短 -> 余量退出
        old2 = self.make_dataset("old2.db", [("a", [(1, 5, 8)])])
        new2 = self.make_dataset("new2.db", [("a", [(1, 5, 5)])])
        out2, _ = self.build(old2, new2, self.write_manifest([], "m2.jsonl"),
                             "ledger2.db")
        with Ledger(out2) as led:
            self.assertEqual(ledger_tuples(led), sorted([
                ("carry", ("a", 1), 0, 5),
                ("exit", ("a", 1), 5, 3),
            ]))
            led.verify()


# ------------------------------------------------------------ 展开模型交叉核对

class RandomizedCrossCheckTest(LedgerTestCase):
    """随机两期数据 + 随机合法清单：明细与地区合计全量对照展开模型。"""

    def random_case(self, seed):
        rng = random.Random(seed)
        senders = [f"r{i:02d}" for i in range(rng.randint(2, 5))]

        def gen_rows():
            rows = {}
            for s in senders:
                for r in rng.sample(range(1, 30), rng.randint(1, 5)):
                    val = rng.choice([None, -5, 0, 3, 7, 100])
                    rows[(s, r)] = (val, rng.randint(1, 12))
            return rows

        old_rows, new_rows = gen_rows(), gen_rows()
        old_used, new_used, decls = {}, {}, []
        for _ in range(rng.randint(0, 60)):
            okey = rng.choice(list(old_rows))
            nkey = rng.choice(list(new_rows))
            ow, nw = old_rows[okey][1], new_rows[nkey][1]
            length = rng.randint(1, 5)
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
                    decls.append((len(decls) + 1,
                                  (okey[0], okey[1], os_),
                                  (nkey[0], nkey[1], ns_), length))
                    break
        return old_rows, new_rows, decls

    def check_case(self, seed):
        old_rows, new_rows, decls = self.random_case(seed)
        old = self.make_dataset(f"old{seed}.db", rows_to_shards(old_rows))
        new = self.make_dataset(f"new{seed}.db", rows_to_shards(new_rows))
        manifest = self.write_manifest(manifest_lines(decls),
                                       f"m{seed}.jsonl")
        out, summary = self.build(old, new, manifest, f"ledger{seed}.db")
        model = LedgerModel(old_rows, new_rows, decls)
        with Ledger(out) as led:
            self.assertEqual(ledger_tuples(led), model.entries())
            rt = {(r["old_sender"], r["new_sender"]): r["weight"]
                  for r in led.region_totals()}
            self.assertEqual(rt, model.region_totals())
            # 按侧汇总 == 该期数据集各地区总权重
            for period, path in ((0, old), (1, new)):
                ds = Dataset(path)
                self.addCleanup(ds.close)
                expect = {i["sender"]: i["total_weight"]
                          for i in ds.shard_info()}
                self.assertEqual(led.sender_totals(period), expect)
                ds.close()
            v = led.verify()
        self.assertEqual(v["entries"], summary["entries"])
        self.assertEqual(v["kinds"], summary["kinds"])

    def test_seeds(self):
        for seed in (1, 2, 3, 4):
            with self.subTest(seed=seed):
                self.check_case(seed)

    def test_unordered_manifest(self):
        # 清单无序（同一记录的声明不相邻）：结果与有序一致
        old_rows, new_rows, decls = self.random_case(7)
        self.assertGreater(len(decls), 4)
        old = self.make_dataset("oldu.db", rows_to_shards(old_rows))
        new = self.make_dataset("newu.db", rows_to_shards(new_rows))
        pairs = [(d, decl_json(d[1], d[2], d[3])) for d in decls]
        rng = random.Random(99)
        rng.shuffle(pairs)
        lines = [text for _d, text in pairs]
        # 打乱后行号变位置：模型按新行号重记
        renumbered = [(i + 1, d[1], d[2], d[3])
                      for i, (d, _text) in enumerate(pairs)]
        manifest = self.write_manifest(lines, "mu.jsonl")
        out, _ = self.build(old, new, manifest, "ledgeru.db")
        model = LedgerModel(old_rows, new_rows, renumbered)
        with Ledger(out) as led:
            self.assertEqual(ledger_tuples(led), model.entries())
            led.verify()


# ------------------------------------------------------------ 冲突与坏行

class ConflictTest(LedgerTestCase):
    def make_pair(self):
        old = self.make_dataset("old.db", [("a", [(1, 1, 200)]),
                                           ("c", [(1, 1, 5)])])
        new = self.make_dataset("new.db", [("b", [(1, 1, 200)])])
        return old, new

    def test_late_conflict_leaves_nothing(self):
        old, new = self.make_pair()
        good = [decl_json(("a", 1, 2 * i), ("b", 1, 2 * i), 2)
                for i in range(50)]
        tails = [
            decl_json(("a", 1, 0), ("b", 1, 100), 2),   # 旧端与第 1 行重叠
            decl_json(("a", 1, 150), ("b", 1, 0), 2),   # 新端与第 1 行重叠
            decl_json(("a", 1, 199), ("b", 1, 150), 2),  # 旧端越界
            decl_json(("a", 1, 150), ("b", 1, 199), 2),  # 新端越界
            decl_json(("a", 99, 0), ("b", 1, 150), 2),   # 旧端记录不存在
            decl_json(("a", 1, 150), ("b", 99, 0), 2),   # 新端记录不存在
            decl_json(("zz", 1, 0), ("b", 1, 150), 2),   # 旧端地区不存在
            "这不是 JSON",
        ]
        for i, bad in enumerate(tails):
            with self.subTest(case=i):
                lines = good + [bad]
                manifest = self.write_manifest(lines, f"m{i}.jsonl")
                out = self.path(f"late{i}.db")
                with self.assertRaises(LedgerError) as ctx:
                    build_ledger(old, new, manifest, out)
                self.assertIn(f"第 {len(lines)} 行", str(ctx.exception))
                # 冲突在末尾：不交付、不留半成品
                self.assertFalse(out.exists())
                self.assertEqual(self.tmp_files(), [])

    def test_duplicate_declaration_conflicts(self):
        old, new = self.make_pair()
        d = decl_json(("a", 1, 0), ("b", 1, 0), 2)
        manifest = self.write_manifest([d, d])
        with self.assertRaises(LedgerError) as ctx:
            build_ledger(old, new, manifest, self.path("dup.db"))
        self.assertIn("第 2 行", str(ctx.exception))
        self.assertIn("第 1 行", str(ctx.exception))

    def test_adjacent_claims_legal(self):
        old, new = self.make_pair()
        lines = [decl_json(("a", 1, 0), ("b", 1, 0), 3),
                 decl_json(("a", 1, 3), ("b", 1, 3), 2)]  # 首尾相接
        manifest = self.write_manifest(lines)
        out, summary = self.build(old, new, manifest)
        self.assertEqual(summary["kinds"]["transfer"], 2)
        with Ledger(out) as led:
            self.assertEqual(ledger_tuples(led), sorted([
                ("transfer", ("a", 1), 0, ("b", 1), 0, 3, 1),
                ("transfer", ("a", 1), 3, ("b", 1), 3, 2, 2),
                ("exit", ("a", 1), 5, 195),
                ("exit", ("c", 1), 0, 5),
                ("new", ("b", 1), 5, 195),
            ]))
            led.verify()

    def test_bad_lines(self):
        old, new = self.make_pair()
        good_old = {"sender": "a", "record_no": 1, "start": "0"}
        good_new = {"sender": "b", "record_no": 1, "start": "0"}

        def line(old_ep=good_old, new_ep=good_new, length="1"):
            return json.dumps({"old": old_ep, "new": new_ep,
                               "length": length})

        cases = [
            "not json",
            "[1, 2]",
            json.dumps({"new": good_new, "length": "1"}),       # 缺旧端
            json.dumps({"old": good_old, "length": "1"}),       # 缺新端
            json.dumps({"old": good_old, "new": good_new}),     # 缺长度
            line(length="0"),                                   # 长度非正
            line(length="-1"),
            line(length="abc"),
            line(length=3),                                     # 长度非字符串
            line(old_ep={**good_old, "start": 0}),              # 起始位非字符串
            line(old_ep={**good_old, "start": "-1"}),
            line(old_ep={**good_old, "start": "1.5"}),
            line(old_ep={**good_old, "record_no": "1"}),
            line(old_ep={**good_old, "record_no": True}),
            line(old_ep={**good_old, "record_no": -1}),
            line(old_ep={**good_old, "sender": ""}),
            line(old_ep={**good_old, "sender": 123}),
            line(new_ep={**good_new, "start": "199"},
                 length="2"),                                   # 新端越界
        ]
        for i, bad in enumerate(cases):
            with self.subTest(case=i):
                manifest = self.write_manifest([bad], f"bad{i}.jsonl")
                with self.assertRaises(LedgerError) as ctx:
                    build_ledger(old, new, manifest, self.path(f"bad{i}.db"))
                self.assertIn("第 1 行", str(ctx.exception))
                self.assertFalse(self.path(f"bad{i}.db").exists())

    def test_blank_lines_count_in_line_numbers(self):
        old, new = self.make_pair()
        lines = ["", decl_json(("a", 1, 0), ("b", 1, 0), 2),
                 "", decl_json(("a", 1, 2), ("b", 1, 2), 2)]
        manifest = self.write_manifest(lines)
        out, summary = self.build(old, new, manifest)
        self.assertEqual(summary["declarations"], 2)
        with Ledger(out) as led:
            self.assertEqual(sorted(e["line_no"]
                                    for e in led.entries("transfer")),
                             [2, 4])  # 空行也占行号
            led.verify()


# ------------------------------------------------------------ 交付语义

class DeliveryTest(LedgerTestCase):
    def test_existing_target_not_overwritten(self):
        old = self.make_dataset("old.db", [("a", [(1, 1, 3)])])
        new = self.make_dataset("new.db", [("a", [(1, 1, 3)])])
        manifest = self.write_manifest([])
        out = self.path("keep.db")
        out.write_bytes(b"keep me")
        with self.assertRaises(LedgerError):
            build_ledger(old, new, manifest, out)
        self.assertEqual(out.read_bytes(), b"keep me")
        self.assertEqual(self.tmp_files(), [])

    def test_missing_inputs(self):
        old = self.make_dataset("old.db", [("a", [(1, 1, 3)])])
        manifest = self.write_manifest([])
        with self.assertRaises(LedgerError):
            build_ledger(self.path("nope.db"), old, manifest,
                         self.path("x.db"))
        with self.assertRaises(LedgerError):
            build_ledger(old, old, self.path("nope.jsonl"),
                         self.path("x.db"))
        with self.assertRaises(LedgerError):
            build_ledger(old, old, manifest, self.path("no_dir/y.db"))

    def test_inputs_not_modified(self):
        old = self.make_dataset("old.db", [("a", [(1, 1, 10), (2, 2, 2)])])
        new = self.make_dataset("new.db", [("a", [(1, 1, 10)]), ])
        digest = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
        before = (digest(old), digest(new))
        manifest = self.write_manifest(
            [decl_json(("a", 1, 0), ("a", 1, 5), 4)])
        self.build(old, new, manifest)
        self.assertEqual(before, (digest(old), digest(new)))
        # 两份库仍是可正常查询的数据集
        ds = Dataset(old)
        self.addCleanup(ds.close)
        self.assertEqual(ds.total_weight(), 12)

    def test_ledger_independent_of_inputs(self):
        # 建台账后删除输入库与清单，台账仍可独立核对
        inputs = self.path("inputs")
        inputs.mkdir()
        old_p = inputs / "old.db"
        new_p = inputs / "new.db"
        Dataset.create(old_p, [("a", [(1, 1, 6)]), ("b", [(1, 2, 4)])]).close()
        Dataset.create(new_p, [("a", [(1, 1, 8)])]).close()
        manifest = inputs / "m.jsonl"
        manifest.write_text(decl_json(("a", 1, 0), ("a", 1, 6), 2) + "\n",
                            encoding="utf-8")
        out = self.path("ledger.db")
        summary = build_ledger(old_p, new_p, manifest, out)
        shutil.rmtree(inputs)  # 输入全部不可用
        with Ledger(out) as led:
            v = led.verify()
            self.assertEqual(v["kinds"], summary["kinds"])
            self.assertEqual(led.sender_totals(0), {"a": 6, "b": 4})
            self.assertEqual(led.sender_totals(1), {"a": 8})
            self.assertEqual(v["total_weight"], {"old": 10, "new": 8})
            meta = led.meta()
            self.assertEqual(meta["manifest_lines"], "1")
            self.assertEqual(len(meta["manifest_sha256"]), 64)

    def test_empty_manifest_and_empty_datasets(self):
        old = self.make_dataset("old.db", [])
        new = self.make_dataset("new.db", [])
        out, summary = self.build(old, new, self.write_manifest(["", "  "]))
        self.assertEqual(summary["entries"], 0)
        with Ledger(out) as led:
            self.assertEqual(list(led.entries()), [])
            self.assertEqual(led.region_totals(), [])
            self.assertEqual(led.sender_totals(0), {})
            v = led.verify()
            self.assertEqual(v["total_weight"], {"old": 0, "new": 0})


# ------------------------------------------------------------ 大权重与集中声明

class BigAndConcentratedTest(LedgerTestCase):
    def test_big_weights_and_positions(self):
        BIG = 2 ** 63 + 7
        old = self.make_dataset("old.db", [
            ("m", [(1, 10, str(BIG))]),
            ("huge", [(1, 1, str(10 ** 15))]),
        ])
        new = self.make_dataset("new.db", [
            ("m", [(1, 10, str(BIG + 100))]),
            ("huge", [(1, 1, str(10 ** 15))]),
        ])
        # 声明：旧 m1[2^63, 2^63+5) -> 新 m1[2^63+6, 2^63+11)
        manifest = self.write_manifest([
            decl_json(("m", 1, 2 ** 63), ("m", 1, 2 ** 63 + 6), 5)])
        out, summary = self.build(old, new, manifest)
        # 旧空闲 [0,2^63)∪[2^63+5,2^63+7)；新空闲 [0,2^63+6)∪[2^63+11,2^63+107)
        expected = sorted([
            ("transfer", ("m", 1), 2 ** 63, ("m", 1), 2 ** 63 + 6, 5, 1),
            ("carry", ("m", 1), 0, 2 ** 63),
            ("carry", ("m", 1), 2 ** 63 + 5, 1),
            ("exit", ("m", 1), 2 ** 63 + 6, 1),
            ("new", ("m", 1), 2 ** 63, 5),
            ("new", ("m", 1), 2 ** 63 + 11, 96),
            ("carry", ("huge", 1), 0, 10 ** 15),
        ])
        with Ledger(out) as led:
            self.assertEqual(ledger_tuples(led), expected)
            self.assertEqual(led.sender_totals(0),
                             {"m": BIG, "huge": 10 ** 15})
            self.assertEqual(led.sender_totals(1),
                             {"m": BIG + 100, "huge": 10 ** 15})
            v = led.verify()
        self.assertEqual(v["total_weight"],
                         {"old": BIG + 10 ** 15, "new": BIG + 100 + 10 ** 15})
        # 库内副本位/合计是精确十进制文本，不经过 int64
        import sqlite3
        conn = sqlite3.connect(f"file:{out}?mode=ro", uri=True)
        try:
            row = conn.execute(
                "SELECT old_start, length FROM entries WHERE kind = 'exit'"
            ).fetchone()
            self.assertEqual(row, (str(2 ** 63 + 6), "1"))
            row = conn.execute(
                "SELECT weight FROM sender_totals WHERE period = 1"
                " AND sender = 'm'").fetchone()
            self.assertEqual(row[0], str(BIG + 100))
        finally:
            conn.close()

    def test_concentrated_declarations(self):
        # 大量声明集中在同一记录上：流式处理，不攒全量声明
        W = 120_000
        N = 40_000
        old = self.make_dataset("old.db", [
            ("hot", [(1, 1, W)]), ("side", [(1, 1, 3)])])
        new = self.make_dataset("new.db", [
            ("hot2", [(1, 1, W)]), ("side", [(1, 1, 3)])])
        manifest = self.path("dense.jsonl")
        with open(manifest, "w", encoding="utf-8") as fh:
            for i in range(N):
                fh.write(decl_json(("hot", 1, 3 * i),
                                   ("hot2", 1, 3 * i), 3) + "\n")
        out, summary = self.build(old, new, manifest)
        self.assertEqual(summary["kinds"],
                         {"transfer": N, "carry": 1, "exit": 0, "new": 0})
        self.assertEqual(summary["total_weight"],
                         {"old": W + 3, "new": W + 3})
        with Ledger(out) as led:
            self.assertEqual(led.sender_totals(0),
                             {"hot": W, "side": 3})
            self.assertEqual(led.sender_totals(1),
                             {"hot2": W, "side": 3})
            rt = {(r["old_sender"], r["new_sender"]): r["weight"]
                  for r in led.region_totals()}
            self.assertEqual(rt, {("hot", "hot2"): W, ("side", "side"): 3})
            # 抽查一条转列可回查清单行
            some = [e for e in led.entries("transfer") if e["line_no"] == N]
            self.assertEqual(len(some), 1)
            self.assertEqual(some[0]["old"]["start"], 3 * (N - 1))
            v = led.verify()
            self.assertEqual(v["entries"], N + 1)

    def test_many_records_streamed(self):
        # 记录数较多：归并不在内存攒全量源记录
        rng = random.Random(5)
        old_rows, new_rows = {}, {}
        for i in range(4_000):
            s = f"s{i % 40:02d}"
            old_rows[(s, i)] = (i % 7, rng.randint(1, 9))
            if i % 3:
                new_rows[(s, i)] = (i % 7, rng.randint(1, 9))
            else:
                new_rows[(s, 10_000 + i)] = (i % 7, rng.randint(1, 9))
        old = self.make_dataset("old.db", rows_to_shards(old_rows))
        new = self.make_dataset("new.db", rows_to_shards(new_rows))
        out, summary = self.build(old, new, self.write_manifest([]))
        model = LedgerModel(old_rows, new_rows, [])
        with Ledger(out) as led:
            self.assertEqual(ledger_tuples(led), model.entries())
            v = led.verify()
        self.assertEqual(v["records"], summary["records"])


if __name__ == "__main__":
    unittest.main()
