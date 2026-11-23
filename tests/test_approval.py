# -*- coding: utf-8 -*-
"""审批合并的测试。

含一个逐副本展开的独立参照模型：从三份台账的 transfer 明细展开每个旧端
副本位的三稿指向，按审批规则逐位判定，再按合段规则分组，与
merge_approvals 的交付文件（合并清单或争议明细）全量对照。
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

from ordered_exchange import (ApprovalError, Dataset, Ledger, STATUS_APPROVED,
                              STATUS_DISPUTED, build_ledger, merge_approvals)


# ------------------------------------------------------------ 参照模型

class ApprovalModel:
    """逐副本展开的独立参照（仅适用小权重）。"""

    def __init__(self, base, left, right):
        # 记录登记：period -> {(sender, rec): (is_unknown, valuation, weight)}
        self.records = ({}, {})
        for period in (0, 1):
            for r in Ledger(base).records(period):
                self.records[period][(r["sender"], r["record_no"])] = (
                    int(r["is_unknown"]), r["value"], r["weight"])
        # 每稿每旧端记录的指向表：(sender, rec) -> {pos: (ns,nr,npos)}
        self.maps = []
        for path in (base, left, right):
            m = {}
            with Ledger(path) as led:
                for e in led.entries("transfer"):
                    key = (e["old"]["sender"], e["old"]["record_no"])
                    for k in range(e["length"]):
                        m.setdefault(key, {})[e["old"]["start"] + k] = (
                            e["new"]["sender"], e["new"]["record_no"],
                            e["new"]["start"] + k)
            self.maps.append(m)

    @staticmethod
    def _classify(t0, t1, t2):
        """三稿指向 -> ("adopt", 指向|None) 或 ("dispute", (t0,t1,t2))。"""
        if t1 == t2:
            return "adopt", t1
        if t1 == t0:
            return "adopt", t2
        if t2 == t0:
            return "adopt", t1
        return "dispute", (t0, t1, t2)

    def expected(self):
        """逐位判定并合段，返回 (状态, 期望清单行, 期望争议行)。"""
        resolved = []      # (o_sender, o_rec, o_start, target, length 段)
        old_elems = []     # (sender, rec, start, end, (t0,t1,t2))
        for key in sorted(self.records[0]):
            weight = self.records[0][key][2]
            pending = None   # (lo, kind, payload)
            for pos in range(weight):
                targets = tuple(self.maps[i].get(key, {}).get(pos)
                                for i in range(3))
                if all(t is None for t in targets):
                    kind, payload = "gap", None
                else:
                    kind, payload = self._classify(*targets)
                cur = (pos, kind, payload)
                if pending is None:
                    pending = [pos, pos + 1, kind, payload]
                    continue
                _plo, phi, pk, pp = pending
                if pk == kind and pp == payload:
                    pending[1] = pos + 1
                    continue
                self._flush_model(pending, key, resolved, old_elems)
                pending = [pos, pos + 1, kind, payload]
            if pending is not None:
                self._flush_model(pending, key, resolved, old_elems)
        # 新端占用：new_key -> {npos: [(o_sender,o_rec,opos)]}
        occ_map = {}
        for o_sender, o_rec, o_start, target, length in resolved:
            n_sender, n_rec, n_start = target
            for k in range(length):
                occ_map.setdefault((n_sender, n_rec), {}) \
                    .setdefault(n_start + k, []).append(
                    (o_sender, o_rec, o_start + k))
        new_elems = []
        for key in sorted(occ_map):
            positions = occ_map[key]
            pending = None   # (lo, hi, occupant 元组集合)
            for npos in range(self.records[1][key][2]):
                occ = tuple(sorted(positions.get(npos, ())))
                if len(occ) < 2:
                    if pending is not None:
                        new_elems.append((key[0], key[1], pending[0],
                                          pending[1], pending[2]))
                        pending = None
                    continue
                if pending is not None and pending[2] == occ \
                        and pending[1] == npos:
                    pending[1] = npos + 1
                else:
                    if pending is not None:
                        new_elems.append((key[0], key[1], pending[0],
                                          pending[1], pending[2]))
                    pending = [npos, npos + 1, occ]
            if pending is not None:
                new_elems.append((key[0], key[1], pending[0], pending[1],
                                  pending[2]))
        old_lines = self._merge_old(old_elems)
        new_lines = self._merge_new(new_elems)
        if old_lines or new_lines:
            return STATUS_DISPUTED, [], old_lines + new_lines
        manifest = self._build_manifest(resolved)
        return STATUS_APPROVED, manifest, []

    @staticmethod
    def _flush_model(pending, key, resolved, old_elems):
        lo, hi, kind, payload = pending
        if kind == "dispute":
            old_elems.append((key[0], key[1], lo, hi, payload))
        elif kind == "adopt" and payload is not None:
            resolved.append((key[0], key[1], lo, payload, hi - lo))

    @staticmethod
    def _point(target):
        if target is None:
            return None
        return {"sender": target[0], "record_no": target[1],
                "start": str(target[2])}

    def _merge_old(self, elems):
        out = []
        for sender, rec, lo, hi, targets in elems:
            if out and out[-1]["_key"] == (sender, rec) \
                    and out[-1]["_end"] == lo \
                    and self._old_compatible(out[-1]["_options"], targets,
                                             lo - out[-1]["_start"]):
                out[-1]["old"]["length"] = str(
                    int(out[-1]["old"]["length"]) + (hi - lo))
                out[-1]["_end"] = hi
                continue
            out.append({
                "type": "old",
                "old": {"sender": sender, "record_no": rec,
                        "start": str(lo), "length": str(hi - lo)},
                "options": {"base": self._point(targets[0]),
                            "left": self._point(targets[1]),
                            "right": self._point(targets[2])},
                "_key": (sender, rec), "_start": lo, "_end": hi,
                "_options": targets})
        for line in out:
            for k in ("_key", "_start", "_end", "_options"):
                del line[k]
        return out

    @staticmethod
    def _old_compatible(prev, cur, shift):
        for a, b in zip(prev, cur):
            if a is None or b is None:
                if a is not None or b is not None:
                    return False
            elif a[0] != b[0] or a[1] != b[1] or a[2] + shift != b[2]:
                return False
        return True

    def _merge_new(self, elems):
        out = []
        for sender, rec, lo, hi, occ in elems:
            occupants = [{"old": {"sender": s, "record_no": r,
                                  "start": str(p), "length": str(hi - lo)}}
                         for s, r, p in occ]
            if out and out[-1]["_key"] == (sender, rec) \
                    and out[-1]["_end"] == lo \
                    and self._new_compatible(out[-1]["_occ"], occ,
                                             lo - out[-1]["_start"]):
                out[-1]["new"]["length"] = str(
                    int(out[-1]["new"]["length"]) + (hi - lo))
                for item, oc in zip(out[-1]["occupants"], occ):
                    item["old"]["length"] = str(
                        int(item["old"]["length"])
                        + (hi - out[-1]["_prev_hi"]))
                out[-1]["_end"] = hi
                out[-1]["_prev_hi"] = hi
                continue
            out.append({"type": "new",
                        "new": {"sender": sender, "record_no": rec,
                                "start": str(lo), "length": str(hi - lo)},
                        "occupants": occupants,
                        "_key": (sender, rec), "_start": lo, "_end": hi,
                        "_occ": occ, "_prev_hi": hi})
        for line in out:
            for k in ("_key", "_start", "_end", "_occ", "_prev_hi"):
                del line[k]
        return out

    @staticmethod
    def _new_compatible(prev_occ, cur_occ, shift):
        if len(prev_occ) != len(cur_occ):
            return False
        for a, b in zip(prev_occ, cur_occ):
            if a[0] != b[0] or a[1] != b[1] or a[2] + shift != b[2]:
                return False
        return True

    @staticmethod
    def _build_manifest(resolved):
        """按旧端排序并合段，生成 build_ledger 清单行。"""
        resolved.sort(key=lambda x: (x[0], x[1], x[2]))
        out = []
        pending = None
        for o_sender, o_rec, o_start, target, length in resolved:
            n_sender, n_rec, n_start = target
            if pending is not None \
                    and pending["o"] == (o_sender, o_rec) \
                    and pending["o_end"] == o_start \
                    and pending["n"] == (n_sender, n_rec) \
                    and pending["n_end"] == n_start:
                pending["o_end"] = o_start + length
                pending["n_end"] = n_start + length
                continue
            if pending is not None:
                out.append(ApprovalModel._manifest_line(pending))
            pending = {"o": (o_sender, o_rec), "o_end": o_start + length,
                       "n": (n_sender, n_rec), "n_end": n_start + length,
                       "o_start": o_start, "n_start": n_start}
        if pending is not None:
            out.append(ApprovalModel._manifest_line(pending))
        return out

    @staticmethod
    def _manifest_line(p):
        length = p["o_end"] - p["o_start"]
        return {"old": {"sender": p["o"][0], "record_no": p["o"][1],
                        "start": str(p["o_start"])},
                "new": {"sender": p["n"][0], "record_no": p["n"][1],
                        "start": str(p["n_end"] - length)},
                "length": str(length)}


# ------------------------------------------------------------ 工具

def rows_to_shards(rows):
    by_sender = {}
    for (s, r), (v, w) in rows.items():
        by_sender.setdefault(s, []).append((r, v, w))
    return [(s, sorted(rs, key=lambda t: (t[1] is None,
                                          t[1] if t[1] is not None else 0)))
            for s, rs in sorted(by_sender.items())]


def decl_json(o, n, length):
    return json.dumps({
        "old": {"sender": o[0], "record_no": o[1], "start": str(o[2])},
        "new": {"sender": n[0], "record_no": n[1], "start": str(n[2])},
        "length": str(length)}, ensure_ascii=False)


class ApprovalCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="oappr_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def path(self, name):
        return self.tmp / name

    def make_dataset(self, name, shards):
        p = self.path(name)
        Dataset.create(p, shards).close()
        return p

    def write_manifest(self, lines, name):
        p = self.path(name)
        with open(p, "w", encoding="utf-8") as fh:
            for ln in lines:
                fh.write(ln if ln.endswith("\n") else ln + "\n")
        return p

    def build(self, old, new, lines, name):
        manifest = self.write_manifest(lines, name + ".jsonl")
        out = self.path(name + ".db")
        build_ledger(old, new, manifest, out)
        return out

    def merge(self, base, left, right, m="manifest.jsonl", d="disputes.jsonl"):
        mp, dp = self.path(m), self.path(d)
        summary = merge_approvals(base, left, right, mp, dp)
        return summary, mp, dp

    def tmp_files(self):
        return [f for f in os.listdir(self.tmp) if f.endswith(".tmp")]

    def read_jsonl(self, path):
        if not path.exists():
            return None
        return [json.loads(ln) for ln in path.read_text(
            encoding="utf-8").splitlines() if ln]


# ------------------------------------------------------------ 判定规则

class ClassificationTest(ApprovalCase):
    def setUp(self):
        super().setUp()
        # 旧 A/B 各 12；新 C/D 各 12，足够容纳各种改派
        self.old = self.make_dataset("old.db", [
            ("A", [(1, 1, 12)]), ("B", [(1, 1, 12)])])
        self.new = self.make_dataset("new.db", [
            ("C", [(1, 1, 12)]), ("D", [(1, 1, 12)])])

    def ledgers(self, base, left, right):
        return (self.build(self.old, self.new, base, "b"),
                self.build(self.old, self.new, left, "l"),
                self.build(self.old, self.new, right, "r"))

    def test_both_drafts_agree_adopted(self):
        # 双稿一致改派 D[0,4)，base 指向 C
        b, l, r = self.ledgers(
            [decl_json(("A", 1, 0), ("C", 1, 0), 4)],
            [decl_json(("A", 1, 0), ("D", 1, 0), 4)],
            [decl_json(("A", 1, 0), ("D", 1, 0), 4)])
        summary, mp, dp = self.merge(b, l, r)
        self.assertEqual(summary["status"], STATUS_APPROVED)
        self.assertEqual(self.read_jsonl(mp), [
            {"old": {"sender": "A", "record_no": 1, "start": "0"},
             "new": {"sender": "D", "record_no": 1, "start": "0"},
             "length": "4"}])
        self.assertFalse(dp.exists())

    def test_both_withdraw_adopted_without_output(self):
        # 双稿都撤回 base 的 A[0,4)：采用撤回，该区间不出现在清单
        b, l, r = self.ledgers(
            [decl_json(("A", 1, 0), ("C", 1, 0), 4)], [], [])
        summary, mp, dp = self.merge(b, l, r)
        self.assertEqual(summary["status"], STATUS_APPROVED)
        self.assertEqual(summary["declarations"], 0)
        self.assertEqual(self.read_jsonl(mp), [])

    def test_single_draft_change_adopted(self):
        # 仅左稿把 A[0,4) 改派 D；右稿与 base 相同
        b, l, r = self.ledgers(
            [decl_json(("A", 1, 0), ("C", 1, 0), 4)],
            [decl_json(("A", 1, 0), ("D", 1, 0), 4)],
            [decl_json(("A", 1, 0), ("C", 1, 0), 4)])
        summary, mp, _ = self.merge(b, l, r)
        self.assertEqual(summary["status"], STATUS_APPROVED)
        self.assertEqual(self.read_jsonl(mp)[0]["new"]["sender"], "D")

    def test_single_draft_withdrawal_adopted(self):
        # 仅右稿撤回 A[0,4)，左稿与 base 相同：采用撤回
        b, l, r = self.ledgers(
            [decl_json(("A", 1, 0), ("C", 1, 0), 4)],
            [decl_json(("A", 1, 0), ("C", 1, 0), 4)], [])
        summary, mp, dp = self.merge(b, l, r)
        self.assertEqual(summary["status"], STATUS_APPROVED)
        self.assertEqual(self.read_jsonl(mp), [])
        self.assertFalse(dp.exists())

    def test_both_changed_differently_is_old_dispute(self):
        b, l, r = self.ledgers(
            [decl_json(("A", 1, 0), ("C", 1, 0), 6)],
            [decl_json(("A", 1, 0), ("D", 1, 0), 6)],
            [decl_json(("A", 1, 0), ("D", 1, 4), 6)])
        summary, mp, dp = self.merge(b, l, r)
        self.assertEqual(summary["status"], STATUS_DISPUTED)
        self.assertFalse(mp.exists())
        lines = self.read_jsonl(dp)
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0]["type"], "old")
        self.assertEqual(lines[0]["options"], {
            "base": {"sender": "C", "record_no": 1, "start": "0"},
            "left": {"sender": "D", "record_no": 1, "start": "0"},
            "right": {"sender": "D", "record_no": 1, "start": "4"}})

    def test_withdrawal_vs_repoint_is_old_dispute(self):
        # 一方明确撤回、另一方改派，不能自动合并
        b, l, r = self.ledgers(
            [decl_json(("A", 1, 0), ("C", 1, 0), 6)],
            [],
            [decl_json(("A", 1, 0), ("D", 1, 0), 6)])
        summary, _mp, dp = self.merge(b, l, r)
        self.assertEqual(summary["status"], STATUS_DISPUTED)
        line = self.read_jsonl(dp)[0]
        self.assertEqual(line["options"]["left"], None)
        self.assertEqual(line["options"]["right"]["sender"], "D")
        self.assertEqual(line["options"]["base"]["sender"], "C")


class NewSideDisputeTest(ApprovalCase):
    def test_spec_d_example(self):
        # 规格例：左稿 A->D[0,4)，右稿 B[2,6)->D[2,6)，争议仅 D[2,4)
        old = self.make_dataset("o.db", [("A", [(1, 1, 4)]),
                                         ("B", [(1, 1, 6)])])
        new = self.make_dataset("n.db", [("C", [(1, 1, 10)]),
                                         ("D", [(1, 1, 6)])])
        b = self.build(old, new, [
            decl_json(("A", 1, 0), ("C", 1, 0), 4),
            decl_json(("B", 1, 0), ("C", 1, 4), 6)], "b")
        l = self.build(old, new, [
            decl_json(("A", 1, 0), ("D", 1, 0), 4),
            decl_json(("B", 1, 0), ("C", 1, 4), 6)], "l")
        r = self.build(old, new, [
            decl_json(("A", 1, 0), ("C", 1, 0), 4),
            decl_json(("B", 1, 2), ("D", 1, 2), 4)], "r")
        summary, mp, dp = self.merge(b, l, r)
        self.assertEqual(summary["disputes"], {"old": 0, "new": 1})
        self.assertFalse(mp.exists())
        self.assertEqual(self.read_jsonl(dp), [{
            "type": "new",
            "new": {"sender": "D", "record_no": 1,
                    "start": "2", "length": "2"},
            "occupants": [
                {"old": {"sender": "A", "record_no": 1,
                         "start": "2", "length": "2"}},
                {"old": {"sender": "B", "record_no": 1,
                         "start": "2", "length": "2"}}]}])

    def test_same_old_record_distinct_positions_are_distinct_occupants(self):
        # 同一旧端记录的两个位置都落到同一新端副本位：也算两个占用者
        old = self.make_dataset("o.db", [("A", [(1, 1, 8)])])
        new = self.make_dataset("n.db", [("D", [(1, 1, 4)])])
        # 左稿 A[0,4)->D[0,4)；右稿 A[4,8)->D[0,4)，各稿内部不重叠
        b = self.build(old, new, [], "b")
        l = self.build(old, new, [
            decl_json(("A", 1, 0), ("D", 1, 0), 4)], "l")
        r = self.build(old, new, [
            decl_json(("A", 1, 4), ("D", 1, 0), 4)], "r")
        summary, _mp, dp = self.merge(b, l, r)
        self.assertEqual(summary["disputes"], {"old": 0, "new": 1})
        seg = self.read_jsonl(dp)[0]
        self.assertEqual([o["old"]["start"] for o in seg["occupants"]],
                         ["0", "4"])

    def test_old_dispute_positions_excluded_from_new_check(self):
        # A 有旧端争议（不参与新端检查），G/B 各自确定且在新端撞车
        old = self.make_dataset("o.db", [
            ("A", [(1, 1, 6)]), ("B", [(1, 1, 6)]), ("G", [(1, 1, 4)])])
        new = self.make_dataset("n.db", [
            ("D", [(1, 1, 12)]), ("F", [(1, 1, 6)])])
        b = self.build(old, new, [
            decl_json(("A", 1, 0), ("F", 1, 0), 6)], "b")
        l = self.build(old, new, [
            decl_json(("A", 1, 0), ("D", 1, 0), 6),
            decl_json(("G", 1, 0), ("D", 1, 6), 4)], "l")
        r = self.build(old, new, [
            decl_json(("B", 1, 0), ("D", 1, 8), 4)], "r")
        summary, _mp, dp = self.merge(b, l, r)
        self.assertEqual(summary["disputes"], {"old": 1, "new": 1})
        lines = self.read_jsonl(dp)
        self.assertEqual([x["type"] for x in lines], ["old", "new"])
        self.assertEqual(lines[1]["new"]["start"], "8")
        self.assertEqual(sorted(o["old"]["sender"]
                                for o in lines[1]["occupants"]), ["B", "G"])

    def test_adjacent_disputes_merge_and_gap_splits(self):
        # D[2,4) 占用者 {A,B}，D[4,6) 占用者 {B,E}：集合变化，必须分两段
        old = self.make_dataset("o.db", [
            ("A", [(1, 1, 4)]), ("B", [(1, 1, 8)]),
            ("E", [(1, 1, 4)]), ("C", [(1, 1, 4)])])
        new = self.make_dataset("n.db", [("D", [(1, 1, 12)])])
        b = self.build(old, new, [], "b")
        l = self.build(old, new, [
            decl_json(("A", 1, 0), ("D", 1, 0), 4),
            decl_json(("E", 1, 0), ("D", 1, 6), 4)], "l")
        r = self.build(old, new, [
            decl_json(("B", 1, 2), ("D", 1, 2), 4),
            decl_json(("C", 1, 0), ("D", 1, 8), 4)], "r")
        summary, _mp, dp = self.merge(b, l, r)
        segs = self.read_jsonl(dp)
        self.assertEqual([s["new"]["start"] for s in segs], ["2", "8"])
        self.assertEqual([o["old"]["sender"] for s in segs
                          for o in s["occupants"]], ["A", "B", "C", "E"])


# ------------------------------------------------------------ 登记一致性

class RegistrationTest(ApprovalCase):
    def _pair(self, tag):
        old = self.make_dataset(f"old{tag}.db", [("a", [(1, 5, 4)])])
        good = self.make_dataset(f"good{tag}.db", [("a", [(1, 5, 4)])])
        return old, good

    def check_rejected(self, third, period_word, hint):
        old, good = self._pair(period_word + hint)
        b = self.build(old, good, [], "b")
        l = self.build(old, good, [], "l")
        r = self.build(old, third, [], "r")
        mp, dp = self.path("m.jsonl"), self.path("d.jsonl")
        with self.assertRaises(ApprovalError) as ctx:
            merge_approvals(b, l, r, mp, dp)
        msg = str(ctx.exception)
        self.assertIn(period_word, msg)
        self.assertIn(hint, msg)
        self.assertFalse(mp.exists() or dp.exists())
        self.assertEqual(self.tmp_files(), [])

    def test_weight_mismatch(self):
        bad = self.make_dataset("badw.db", [("a", [(1, 5, 5)])])
        self.check_rejected(bad, "新端", "权重")

    def test_unknown_only_equals_unknown(self):
        bad = self.make_dataset("badu.db", [("a", [(1, None, 4)])])
        self.check_rejected(bad, "新端", "估值")

    def test_missing_record(self):
        bad = self.make_dataset("badm.db", [
            ("a", [(1, 5, 4)]), ("z", [(1, 1, 1)])])
        self.check_rejected(bad, "新端", "缺少该记录")

    def test_old_side_mismatch(self):
        good = self.make_dataset("goodold.db", [("a", [(1, 5, 4)])])
        old_bad = self.make_dataset("oldbad.db", [("a", [(1, 5, 5)])])
        old_ok = self.make_dataset("oldok.db", [("a", [(1, 5, 4)])])
        b = self.build(old_bad, good, [], "b")
        l = self.build(old_ok, good, [], "l")
        r = l
        with self.assertRaises(ApprovalError) as ctx:
            merge_approvals(b, l, r, self.path("m.jsonl"),
                            self.path("d.jsonl"))
        self.assertIn("旧端", str(ctx.exception))
        self.assertIn("权重", str(ctx.exception))


# ------------------------------------------------------------ 交付语义

class DeliveryTest(ApprovalCase):
    def make_three(self):
        old = self.make_dataset("old.db", [("a", [(1, 1, 6)])])
        new = self.make_dataset("new.db", [
            ("b", [(1, 1, 6)]), ("c", [(1, 1, 6)])])
        b = self.build(old, new, [
            decl_json(("a", 1, 0), ("b", 1, 0), 6)], "b")
        l = self.build(old, new, [
            decl_json(("a", 1, 0), ("c", 1, 0), 6)], "l")
        r = self.build(old, new, [
            decl_json(("a", 1, 0), ("c", 1, 0), 6)], "r")
        return old, new, b, l, r

    def test_same_output_path_rejected(self):
        _old, _new, b, l, r = self.make_three()
        with self.assertRaises(ApprovalError):
            merge_approvals(b, l, r, self.path("x.jsonl"),
                            self.path("x.jsonl"))

    def test_existing_target_not_overwritten(self):
        _old, _new, b, l, r = self.make_three()
        keep = self.path("keep.jsonl")
        keep.write_bytes(b"keep")
        other = self.path("other.jsonl")
        with self.assertRaises(ApprovalError):
            merge_approvals(b, l, r, keep, other)
        self.assertEqual(keep.read_bytes(), b"keep")
        with self.assertRaises(ApprovalError):
            merge_approvals(b, l, r, other, keep)
        self.assertEqual(keep.read_bytes(), b"keep")
        self.assertEqual(self.tmp_files(), [])

    def test_missing_dir_rejected(self):
        _old, _new, b, l, r = self.make_three()
        with self.assertRaises(ApprovalError):
            merge_approvals(b, l, r, self.path("no_dir/m.jsonl"),
                            self.path("d.jsonl"))

    def test_invalid_ledger_rejected(self):
        _old, _new, b, l, r = self.make_three()
        with self.assertRaises(ApprovalError):
            merge_approvals(self.path("nope.db"), l, r,
                            self.path("m.jsonl"), self.path("d.jsonl"))
        with self.assertRaises(ApprovalError):
            merge_approvals(_old, l, r, self.path("m.jsonl"),
                            self.path("d.jsonl"))  # 数据集不是台账

    def test_inputs_untouched_and_verifiable(self):
        _old, _new, b, l, r = self.make_three()
        digest = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
        before = (digest(b), digest(l), digest(r))
        self.merge(b, l, r)
        self.assertEqual((digest(b), digest(l), digest(r)), before)
        for p in (b, l, r):
            with Ledger(p) as led:
                led.verify()

    def test_manifest_feeds_build_ledger(self):
        old, new, b, l, r = self.make_three()
        summary, mp, _dp = self.merge(b, l, r)
        self.assertEqual(summary["status"], STATUS_APPROVED)
        rebuilt = self.path("rebuilt.db")
        build_ledger(old, new, mp, rebuilt)
        with Ledger(rebuilt) as led:
            verify = led.verify()
            transfers = list(led.entries("transfer"))
        self.assertEqual(verify["kinds"]["transfer"], 1)
        self.assertEqual(transfers[0]["new"]["sender"], "c")

    def test_dispute_delivers_no_partial_manifest(self):
        old = self.make_dataset("old.db", [("A", [(1, 1, 6)])])
        new = self.make_dataset("new.db", [
            ("C", [(1, 1, 6)]), ("D", [(1, 1, 6)])])
        b = self.build(old, new, [
            decl_json(("A", 1, 0), ("C", 1, 0), 6)], "b")
        l = self.build(old, new, [
            decl_json(("A", 1, 0), ("D", 1, 0), 6)], "l")
        r = self.build(old, new, [
            decl_json(("A", 1, 0), ("D", 1, 0), 3),
            decl_json(("A", 1, 3), ("C", 1, 0), 3)], "r")
        summary, mp, dp = self.merge(b, l, r)
        self.assertEqual(summary["status"], STATUS_DISPUTED)
        self.assertFalse(mp.exists())
        self.assertTrue(dp.exists())
        self.assertEqual(self.tmp_files(), [])

    def test_empty_declarations_empty_manifest(self):
        old = self.make_dataset("old.db", [("a", [(1, 1, 3)])])
        new = self.make_dataset("new.db", [("a", [(1, 1, 3)])])
        ledgers = [self.build(old, new, [], f"x{i}") for i in range(3)]
        summary, mp, dp = self.merge(*ledgers)
        self.assertEqual(summary["status"], STATUS_APPROVED)
        self.assertEqual(mp.read_text(encoding="utf-8"), "")
        self.assertFalse(dp.exists())


# ------------------------------------------------------------ 大副本位

class BigPositionTest(ApprovalCase):
    def test_positions_beyond_int64(self):
        BIG = 2 ** 63 + 37
        old = self.make_dataset("old.db", [("M", [(1, 1, BIG)])])
        new = self.make_dataset("new.db", [
            ("M", [(1, 1, BIG)]), ("X", [(1, 1, BIG)])])
        b = self.build(old, new, [
            decl_json(("M", 1, BIG - 10), ("X", 1, BIG - 10), 10)], "b")
        l = self.build(old, new, [
            decl_json(("M", 1, BIG - 10), ("X", 1, 0), 10)], "l")
        r = self.build(old, new, [
            decl_json(("M", 1, BIG - 10), ("X", 1, 0), 10)], "r")
        summary, mp, _dp = self.merge(b, l, r)
        self.assertEqual(summary["status"], STATUS_APPROVED)
        line = self.read_jsonl(mp)[0]
        self.assertEqual(line["old"]["start"], str(BIG - 10))
        self.assertEqual(line["new"]["start"], "0")
        self.assertEqual(line["length"], "10")
        rebuilt = self.path("rebuilt.db")
        build_ledger(old, new, mp, rebuilt)
        with Ledger(rebuilt) as led:
            led.verify()

    def test_big_position_dispute_exact(self):
        BIG = 2 ** 63 + 50
        old = self.make_dataset("old.db", [("M", [(1, 1, BIG)])])
        new = self.make_dataset("new.db", [
            ("C", [(1, 1, BIG + 10)]), ("D", [(1, 1, BIG + 10)])])
        b = self.build(old, new, [
            decl_json(("M", 1, BIG - 8), ("C", 1, BIG - 8), 8)], "b")
        l = self.build(old, new, [
            decl_json(("M", 1, BIG - 8), ("D", 1, BIG - 8), 8)], "l")
        r = self.build(old, new, [
            decl_json(("M", 1, BIG - 8), ("D", 1, BIG - 7), 8)], "r")
        _s, _mp, dp = self.merge(b, l, r)
        line = self.read_jsonl(dp)[0]
        self.assertEqual(line["old"]["start"], str(BIG - 8))
        self.assertEqual(line["old"]["length"], "8")
        self.assertEqual(line["options"]["right"]["start"], str(BIG - 7))


# ------------------------------------------------------------ 热点记录

class HotRecordTest(ApprovalCase):
    def test_dense_approved_merges_to_one_line(self):
        W, N, CH = 120_000, 40_000, 3
        old = self.make_dataset("old.db", [("hot", [(1, 1, W)])])
        new = self.make_dataset("new.db", [("hot2", [(1, 1, W)])])
        lines = [decl_json(("hot", 1, CH * i), ("hot2", 1, CH * i), CH)
                 for i in range(N)]
        ledgers = [self.build(old, new, lines, f"h{i}") for i in range(3)]
        summary, mp, _dp = self.merge(*ledgers)
        self.assertEqual(summary["status"], STATUS_APPROVED)
        self.assertEqual(self.read_jsonl(mp), [{
            "old": {"sender": "hot", "record_no": 1, "start": "0"},
            "new": {"sender": "hot2", "record_no": 1, "start": "0"},
            "length": str(W)}])

    def test_dense_new_disputes_merge_to_one_segment(self):
        W, N, CH = 120_000, 40_000, 3
        old = self.make_dataset("old.db", [
            ("h1", [(1, 1, W)]), ("h2", [(1, 1, W)])])
        new = self.make_dataset("new.db", [("k", [(1, 1, W)])])
        b = self.build(old, new, [], "b")
        l_lines = [decl_json(("h1", 1, CH * i), ("k", 1, CH * i), CH)
                   for i in range(N)]
        r_lines = [decl_json(("h2", 1, CH * i), ("k", 1, CH * i), CH)
                   for i in range(N)]
        l = self.build(old, new, l_lines, "l")
        r = self.build(old, new, r_lines, "r")
        summary, _mp, dp = self.merge(b, l, r)
        self.assertEqual(summary["disputes"], {"old": 0, "new": 1})
        seg = self.read_jsonl(dp)[0]
        self.assertEqual(seg["new"]["length"], str(W))
        self.assertEqual(len(seg["occupants"]), 2)
        self.assertTrue(all(o["old"]["length"] == str(W)
                            for o in seg["occupants"]))

    def test_dense_alternating_old_disputes(self):
        W, N, CH = 120_000, 40_000, 3
        old = self.make_dataset("old.db", [("hot", [(1, 1, W)])])
        new = self.make_dataset("new.db", [
            ("hot2", [(1, 1, W)]), ("hot3", [(1, 1, W + 10)])])

        def manifest(mode):
            out = []
            for i in range(N):
                if mode == "base" or i % 2 == 0:
                    tgt = ("hot2", 1, CH * i)
                elif mode == "left":
                    tgt = ("hot3", 1, CH * i)
                else:
                    tgt = ("hot3", 1, CH * i + 1)
                out.append(decl_json(("hot", 1, CH * i), tgt, CH))
            return out

        b = self.build(old, new, manifest("base"), "b")
        l = self.build(old, new, manifest("left"), "l")
        r = self.build(old, new, manifest("right"), "r")
        summary, _mp, dp = self.merge(b, l, r)
        self.assertEqual(summary["disputes"]["old"], N // 2)
        lines = self.read_jsonl(dp)
        self.assertEqual(len(lines), N // 2)
        self.assertEqual(lines[0]["old"]["start"], str(CH))
        self.assertEqual(lines[0]["options"]["right"]["start"], str(CH + 1))


# ------------------------------------------------------------ 随机交叉核对

class RandomizedCrossCheckTest(ApprovalCase):
    def random_ledgers(self, rng, seed):
        senders_old = [f"o{i}" for i in range(rng.randint(2, 3))]
        senders_new = [f"n{i}" for i in range(rng.randint(2, 3))]
        old_rows, new_rows = {}, {}
        for s in senders_old:
            for r in rng.sample(range(1, 20), rng.randint(1, 4)):
                old_rows[(s, r)] = (rng.choice([None, 0, 5, 9]),
                                    rng.randint(1, 10))
        for s in senders_new:
            for r in rng.sample(range(1, 20), rng.randint(1, 4)):
                new_rows[(s, r)] = (rng.choice([None, 0, 5, 9]),
                                    rng.randint(1, 12))
        old_db = self.make_dataset(f"old{seed}.db", rows_to_shards(old_rows))
        new_db = self.make_dataset(f"new{seed}.db", rows_to_shards(new_rows))

        def random_manifest():
            old_used, new_used, lines = {}, {}, []
            for _ in range(rng.randint(0, 40)):
                okey = rng.choice(list(old_rows))
                nkey = rng.choice(list(new_rows))
                ow, nw = old_rows[okey][1], new_rows[nkey][1]
                length = rng.randint(1, 4)
                if length > ow or length > nw:
                    continue
                for _try in range(20):
                    os_ = rng.randint(0, ow - length)
                    ns_ = rng.randint(0, nw - length)
                    if set(range(os_, os_ + length)) & \
                            old_used.get(okey, set()):
                        continue
                    if set(range(ns_, ns_ + length)) & \
                            new_used.get(nkey, set()):
                        continue
                    old_used.setdefault(okey, set()).update(
                        range(os_, os_ + length))
                    new_used.setdefault(nkey, set()).update(
                        range(ns_, ns_ + length))
                    lines.append(decl_json((okey[0], okey[1], os_),
                                           (nkey[0], nkey[1], ns_), length))
                    break
            return lines

        def revise(base_lines):
            """在原批件基础上做小步修订：多数声明原样保留，少量撤回/改派。

            逐笔维护两侧占用簿记，保证改派后仍是一份合法清单。
            """
            old_used, new_used, out = {}, {}, []

            def free_at(used, key, start, length):
                return not (set(range(start, start + length))
                            & used.get(key, set()))

            def book(okey, os_, nkey, ns_, length):
                old_used.setdefault(okey, set()).update(
                    range(os_, os_ + length))
                new_used.setdefault(nkey, set()).update(
                    range(ns_, ns_ + length))

            def place(length):
                """找一个当前空闲的新端位置；找不到返回 None。"""
                for _try in range(30):
                    nkey = rng.choice(list(new_rows))
                    nw = new_rows[nkey][1]
                    if nw < length:
                        continue
                    cand = rng.randint(0, nw - length)
                    if free_at(new_used, nkey, cand, length):
                        return nkey, cand
                return None

            for line in base_lines:
                obj = json.loads(line)
                okey = (obj["old"]["sender"], obj["old"]["record_no"])
                os_ = int(obj["old"]["start"])
                nkey = (obj["new"]["sender"], obj["new"]["record_no"])
                ns_ = int(obj["new"]["start"])
                length = int(obj["length"])
                roll = rng.random()
                if roll < 0.2:
                    continue  # 撤回：两侧都不登记
                if roll < 0.45:
                    placed = place(length)          # 改派
                elif free_at(new_used, nkey, ns_, length):
                    placed = (nkey, ns_)            # 原样保留
                else:
                    placed = place(length)          # 原位被占：另找空位
                if placed is None:
                    continue                        # 无处可放：视为撤回
                use_nkey, use_ns = placed
                out.append(decl_json(
                    (okey[0], okey[1], os_),
                    (use_nkey[0], use_nkey[1], use_ns), length))
                book(okey, os_, use_nkey, use_ns, length)
            return out

        base_lines = random_manifest()
        b = self.build(old_db, new_db, base_lines, f"b{seed}")
        l = self.build(old_db, new_db, revise(base_lines), f"l{seed}")
        r = self.build(old_db, new_db, revise(base_lines), f"r{seed}")
        return old_db, new_db, (b, l, r)

    def check_seed(self, seed):
        rng = random.Random(seed)
        old_db, new_db, (b, l, r) = self.random_ledgers(rng, seed)
        model = ApprovalModel(b, l, r)
        exp_status, exp_manifest, exp_disputes = model.expected()
        summary, mp, dp = self.merge(b, l, r, f"m{seed}.jsonl",
                                     f"d{seed}.jsonl")
        self.assertEqual(summary["status"], exp_status)
        if exp_status == STATUS_APPROVED:
            self.assertEqual(self.read_jsonl(mp), exp_manifest)
            self.assertFalse(dp.exists())
            # 清单回灌 build_ledger 必须成功
            rebuilt = self.path(f"rebuilt{seed}.db")
            build_ledger(old_db, new_db, mp, rebuilt)
            with Ledger(rebuilt) as led:
                led.verify()
        else:
            self.assertEqual(self.read_jsonl(dp), exp_disputes)
            self.assertFalse(mp.exists())

    def test_seeds(self):
        for seed in range(1, 26):
            with self.subTest(seed=seed):
                self.check_seed(seed)


# ------------------------------------------------------------ 示例可复跑

class ExampleTest(unittest.TestCase):
    def test_example_runs(self):
        script = Path(__file__).resolve().parents[1] / "examples" \
            / "approval_example.py"
        proc = subprocess.run([sys.executable, str(script)],
                              capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn('"status": "待协调"', proc.stdout)


if __name__ == "__main__":
    unittest.main()
