# -*- coding: utf-8 -*-
"""有序访问数据集的测试。

包含一个独立的小数据展开模型：把权重展开成逐副本序列，逐页、
逐值地与数据集实现交叉核对。
"""

import bisect
import hashlib
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ordered_exchange import Dataset, ShardError, load_shard

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


class ExpansionModel:
    """独立参照模型：把分片展开成逐副本的全局有序序列。"""

    def __init__(self, shards):
        # shards: [(sender, [(record_no, value, weight), ...]), ...]
        self.entries = []
        for sender, rows in shards:
            for row_idx, (rec, val, w) in enumerate(rows):
                key = (1, 0) if val is None else (0, val)
                for rep in range(w):
                    self.entries.append((key, sender, row_idx, rep, rec, val, w))
        # 全局次序：估值(未知居末) -> 分片标识 -> 源行号 -> 副本位
        self.entries.sort(key=lambda e: (e[0], e[1], e[2], e[3]))
        self.keys = [e[0] for e in self.entries]

    def total(self):
        return len(self.entries)

    def rank(self, value):
        """并列组半开区间 [lo, hi)；缺席时 lo == hi == 插入位。"""
        key = (1, 0) if value is None else (0, value)
        lo = bisect.bisect_left(self.keys, key)
        hi = bisect.bisect_right(self.keys, key)
        return lo, hi

    def slice(self, pos, length):
        """逐副本展开后按记录聚合成与 Dataset.slice 可比的元组列表。"""
        pos = min(pos, len(self.entries))
        end = min(pos + length, len(self.entries))
        out = []
        i = pos
        while i < end:
            _key, sender, row_idx, _rep, rec, val, w = self.entries[i]
            j = i
            while (j < end and self.entries[j][1] == sender
                   and self.entries[j][2] == row_idx):
                j += 1
            glo, ghi = self.rank(val)
            out.append((sender, rec, row_idx, val, w, (glo, ghi),
                        (self.entries[i][3], self.entries[j - 1][3] + 1)))
            i = j
        return out


def as_tuples(entries):
    return [(e["sender"], e["record_no"], e["source_row"], e["value"],
             e["weight"], e["group"], e["replicas"]) for e in entries]


def load_fixture_dir(name):
    base = FIXTURES / name
    return {p.stem: load_shard(p) for p in sorted(base.glob("*.json"))}


class DatasetTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="ods_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def db(self, name="t.db"):
        return self.tmp / name


class TwoSidesMidBigTest(DatasetTestCase):
    """两边都有同值、中间组总权重超过 64 位的夹具。"""

    BIG = 2 ** 63 + 7  # mid 分片单条权重，已超 SQLite 有符号整数上限

    def setUp(self):
        super().setUp()
        fx = load_fixture_dir("two_sides_mid_big")
        self.ds = Dataset.create(self.db(), [fx["left"], fx["mid"], fx["right"]])
        self.addCleanup(self.ds.close)

    def test_group_intervals_are_big_ints(self):
        r5 = self.ds.rank_interval(5)
        self.assertEqual((r5["start"], r5["end"], r5["weight"]), (0, 7, 7))
        r10 = self.ds.rank_interval(10)
        self.assertEqual(r10["start"], 7)
        self.assertEqual(r10["weight"], 2 + self.BIG + 6)
        self.assertEqual(r10["end"], 2 ** 63 + 22)
        r20 = self.ds.rank_interval(20)
        self.assertEqual((r20["start"], r20["end"]), (2 ** 63 + 22, 2 ** 63 + 23))
        self.assertEqual(self.ds.total_weight(), 2 ** 63 + 23)

    def test_absent_and_unknown_insertion(self):
        r6 = self.ds.rank_interval(6)
        self.assertFalse(r6["present"])
        self.assertEqual((r6["start"], r6["end"]), (7, 7))
        self.assertEqual(r6["insertion"], 7)
        # 没有未知值：未知组的插入位在全体之后
        ru = self.ds.rank_interval(None)
        self.assertFalse(ru["present"])
        self.assertEqual(ru["insertion"], 2 ** 63 + 23)

    def test_slice_across_big_group_boundaries(self):
        # 跨 left->mid 边界（位置 8,9,10）
        got = self.ds.slice(8, 3)
        self.assertEqual(as_tuples(got), [
            ("left", 2, 1, 10, 2, (7, 2 ** 63 + 22), (1, 2)),
            ("mid", 1, 0, 10, self.BIG, (7, 2 ** 63 + 22), (0, 2)),
        ])
        # 跨 mid->right 边界，位置本身超过 2^63
        got = self.ds.slice(2 ** 63 + 15, 4)
        self.assertEqual(as_tuples(got), [
            ("mid", 1, 0, 10, self.BIG, (7, 2 ** 63 + 22),
             (self.BIG - 1, self.BIG)),
            ("right", 2, 1, 10, 6, (7, 2 ** 63 + 22), (0, 3)),
        ])
        # 末尾页被总量截断
        got = self.ds.slice(2 ** 63 + 22, 5)
        self.assertEqual(as_tuples(got), [
            ("mid", 2, 1, 20, 1, (2 ** 63 + 22, 2 ** 63 + 23), (0, 1)),
        ])

    def test_cross_check_between_two_queries(self):
        # 并列秩区间与位置切片互相印证：组内首条记录应出现在组起点
        for value, start, weight in ((5, 0, 7), (10, 7, 2 ** 63 + 15),
                                     (20, 2 ** 63 + 22, 1)):
            ri = self.ds.rank_interval(value)
            self.assertEqual((ri["start"], ri["weight"]), (start, weight))
            head = self.ds.slice(ri["start"], 1)
            self.assertEqual(head[0]["group"], (ri["start"], ri["end"]))
            self.assertEqual(head[0]["value"], value)


class DuplicateIdentityTest(DatasetTestCase):
    """不可合并的重复身份：同身份同内容只算一次，同身份异内容拒绝。"""

    def test_duplicate_declaration_counts_once(self):
        fx = load_fixture_dir("duplicate_identity")
        ds = Dataset.create(self.db(), [fx["alpha"], fx["alpha_copy"],
                                        fx["beta_same_rows"]])
        self.addCleanup(ds.close)
        self.assertEqual(ds.shard_count(), 2)  # 重复声明只算一次
        # 不同分片身份下相同字段值仍分别贡献，不按值去重
        self.assertEqual(ds.total_weight(), 6)
        r1 = ds.rank_interval(1)
        self.assertEqual((r1["start"], r1["weight"]), (0, 4))
        got = ds.slice(0, 4)
        self.assertEqual(as_tuples(got), [
            ("dup", 1, 0, 1, 2, (0, 4), (0, 2)),
            ("other", 1, 0, 1, 2, (0, 4), (0, 2)),
        ])

    def test_same_identity_different_content_rejected(self):
        fx = load_fixture_dir("duplicate_identity")
        ds = Dataset.create(self.db(), [fx["alpha"]])
        self.addCleanup(ds.close)
        with self.assertRaises(ShardError):
            ds.declare_shard(*fx["alpha_conflict"])
        # 拒绝后旧内容仍在
        self.assertEqual(ds.rank_interval(3)["weight"], 1)
        self.assertEqual(ds.total_weight(), 3)

    def test_conflicting_declarations_abort_build(self):
        fx = load_fixture_dir("duplicate_identity")
        with self.assertRaises(ShardError):
            Dataset.create(self.db(), [fx["alpha"], fx["alpha_conflict"]])


class UnknownTailTest(DatasetTestCase):
    """未知值统一并列且排在最后。"""

    def setUp(self):
        super().setUp()
        fx = load_fixture_dir("unknown_tail")
        self.ds = Dataset.create(self.db(), [fx["north"], fx["south"]])
        self.addCleanup(self.ds.close)

    def test_unknown_group_is_last(self):
        ds = self.ds
        self.assertEqual(ds.total_weight(), 11)
        rm = ds.rank_interval(-5)
        self.assertEqual((rm["start"], rm["end"], rm["weight"]), (0, 3, 3))
        r4 = ds.rank_interval(4)
        self.assertEqual((r4["start"], r4["end"], r4["weight"]), (3, 5, 2))
        ru = ds.rank_interval(None)
        self.assertTrue(ru["present"])
        self.assertEqual((ru["start"], ru["end"], ru["weight"]), (5, 11, 6))

    def test_insertion_positions_around_unknown(self):
        self.assertEqual(self.ds.rank_interval(0)["insertion"], 3)
        # 大于所有已知值的缺席值插在未知组之前
        r100 = self.ds.rank_interval(100)
        self.assertFalse(r100["present"])
        self.assertEqual((r100["start"], r100["end"]), (5, 5))
        self.assertEqual(r100["insertion"], 5)
        self.assertEqual(self.ds.rank_interval(-6)["insertion"], 0)

    def test_slice_into_unknown_group(self):
        got = self.ds.slice(4, 4)
        self.assertEqual(as_tuples(got), [
            ("south", 9, 1, 4, 2, (3, 5), (1, 2)),
            ("north", 2, 1, None, 3, (5, 11), (0, 3)),
        ])
        # 未知组内按分片标识、源行号排序
        got = self.ds.slice(5, 6)
        self.assertEqual([(e["sender"], e["record_no"]) for e in got],
                         [("north", 2), ("north", 3), ("south", 8)])


class CorrectionShiftTest(DatasetTestCase):
    """更正排在前头的分片：大量原记录秩平移，且不重读其他分片。"""

    def setUp(self):
        super().setUp()
        fx = load_fixture_dir("correction")
        self.fx = fx
        self.log = []
        self.ds = Dataset.create(
            self.db(),
            [fx["aaa_first"], fx["m_mid"], fx["z_last"]],
            trace=lambda sql, params: self.log.append((sql, params)))
        self.addCleanup(self.ds.close)

    def fingerprint(self, sender):
        rows = self.ds.dump_rows(sender)
        return hashlib.sha256(repr(rows).encode()).hexdigest()

    def test_correction_shifts_ranks(self):
        ds = self.ds
        # 更正前：总量 11，末三页与并列区间
        self.assertEqual(ds.total_weight(), 11)
        before_tail = ds.slice(8, 3)
        self.assertEqual(as_tuples(before_tail), [
            ("z_last", 1, 0, 6, 1, (8, 9), (0, 1)),
            ("z_last", 2, 1, 9, 2, (9, 11), (0, 2)),
        ])
        before_r5 = ds.rank_interval(5)
        self.assertEqual((before_r5["start"], before_r5["end"]), (6, 8))
        before_r9 = ds.rank_interval(9)
        self.assertEqual((before_r9["start"], before_r9["end"]), (9, 11))
        fp_mid = self.fingerprint("m_mid")
        fp_last = self.fingerprint("z_last")
        version_before = ds.dataset_version()

        # 更正排最前的分片 aaa_first（权重 3 -> 9，后续秩整体平移 +6）
        self.log.clear()
        status = ds.correct_shard(*self.fx["aaa_first_v2"])
        self.assertEqual(status, "corrected")
        correction_log = list(self.log)

        self.assertGreater(ds.dataset_version(), version_before)
        self.assertEqual(ds.total_weight(), 17)
        # 并列区间同时平移
        self.assertEqual((ds.rank_interval(1)["start"], ds.rank_interval(1)["end"]),
                         (0, 8))
        self.assertEqual((ds.rank_interval(3)["start"], ds.rank_interval(3)["end"]),
                         (8, 12))
        self.assertEqual((ds.rank_interval(5)["start"], ds.rank_interval(5)["end"]),
                         (12, 14))
        self.assertEqual((ds.rank_interval(9)["start"], ds.rank_interval(9)["end"]),
                         (15, 17))
        # 近末页随之平移：原位置 8..10 现在是 aaa_first 的新记录
        self.assertEqual(as_tuples(ds.slice(8, 3)), [
            ("aaa_first", 2, 1, 3, 4, (8, 12), (0, 3)),
        ])
        # 新的末三页仍是 z_last 的记录，但秩区间整体 +6
        self.assertEqual(as_tuples(ds.slice(14, 3)), [
            ("z_last", 1, 0, 6, 1, (14, 15), (0, 1)),
            ("z_last", 2, 1, 9, 2, (15, 17), (0, 2)),
        ])

        # 与独立展开模型逐页核对更正后的全集
        model = ExpansionModel([self.fx["aaa_first_v2"], self.fx["m_mid"],
                                self.fx["z_last"]])
        for p in range(model.total() + 1):
            for length in range(0, model.total() + 2):
                self.assertEqual(as_tuples(ds.slice(p, length)),
                                 model.slice(p, length),
                                 f"page ({p}, {length})")

        # ---- 明细：更正没有重读/重写其他分片 ----
        # 1) 未更正分片的存储行逐字节未变
        self.assertEqual(self.fingerprint("m_mid"), fp_mid)
        self.assertEqual(self.fingerprint("z_last"), fp_last)
        # 2) 更正期间的 SQL 日志：没有 SELECT 任何行表数据，
        #    也没有任何语句以其他分片标识为参数
        for sql, params in correction_log:
            self.assertNotIn("m_mid", map(str, params), sql)
            self.assertNotIn("z_last", map(str, params), sql)
            upper = sql.upper()
            if upper.startswith("SELECT") and "FROM ROWS" in upper:
                self.fail(f"更正期间读取了行表: {sql}")
            if upper.startswith(("UPDATE ROWS", "DELETE FROM ROWS")):
                self.assertIn("aaa_first", map(str, params), sql)
        # 3) 更正只触发了针对被更正分片的删除与插入
        deletes = [s for s, _ in correction_log
                   if s.upper().startswith("DELETE FROM ROWS")]
        self.assertEqual(len(deletes), 1)

    def test_failed_correction_keeps_old_data(self):
        ds = self.ds
        ds.correct_shard(*self.fx["aaa_first_v2"])
        snapshot = {v: (ds.rank_interval(v)["start"], ds.rank_interval(v)["end"])
                    for v in (1, 3, 5, 6, 9)}
        # 逆序的新版本：完整验证失败，仍用以前的库
        bad_rows = [(1, 5, 1), (2, 1, 1)]
        with self.assertRaises(ShardError):
            ds.correct_shard("aaa_first", bad_rows)
        for v, interval in snapshot.items():
            ri = ds.rank_interval(v)
            self.assertEqual((ri["start"], ri["end"]), interval)
        self.assertEqual(ds.total_weight(), 17)
        # 更正不存在的分片也拒绝
        with self.assertRaises(ShardError):
            ds.correct_shard("no_such", [(1, 1, 1)])


class ExpansionModelCrossCheckTest(DatasetTestCase):
    """独立小数据展开模型：核对每页、单记录内部切分与空并列组插入位。"""

    SHARDS = [
        ("s1", [(3, -1, 2), (1, -1, 1), (2, 0, 3), (9, None, 2)]),
        ("s2", [(5, -1, 1), (4, 2, 2), (6, 2, 1), (8, None, 1), (7, None, 2)]),
        ("s3", [(11, 0, 1), (10, 5, 2)]),
    ]

    def setUp(self):
        super().setUp()
        self.ds = Dataset.create(self.db(), self.SHARDS)
        self.addCleanup(self.ds.close)
        self.model = ExpansionModel(self.SHARDS)

    def test_every_page(self):
        ds, model = self.ds, self.model
        total = model.total()
        self.assertEqual(ds.total_weight(), total)
        for p in range(total + 1):
            for length in range(0, total + 2):
                self.assertEqual(as_tuples(ds.slice(p, length)),
                                 model.slice(p, length),
                                 f"page ({p}, {length})")

    def test_every_value_present_and_absent(self):
        ds, model = self.ds, self.model
        for value in [None, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6]:
            lo, hi = model.rank(value)
            ri = ds.rank_interval(value)
            self.assertEqual((ri["start"], ri["end"]), (lo, hi), value)
            self.assertEqual(ri["weight"], hi - lo, value)
            self.assertEqual(ri["present"], hi > lo, value)
            self.assertEqual(ri["insertion"], lo, value)
            # 交叉核对：存在值的组起点切一条，其组区间与按值查询一致
            if hi > lo:
                head = ds.slice(lo, 1)
                self.assertEqual(head[0]["group"], (lo, hi), value)
                self.assertEqual(head[0]["value"], value)
            elif lo < model.total():
                # 空并列组：插入位恰好是下一组起点
                nxt = ds.slice(lo, 1)
                self.assertEqual(nxt[0]["group"][0], lo, value)

    def test_cut_inside_single_record(self):
        ds = Dataset.create(self.db("single.db"),
                            [("only", [(1, 7, 10)])])
        self.addCleanup(ds.close)
        got = ds.slice(3, 4)
        self.assertEqual(as_tuples(got), [
            ("only", 1, 0, 7, 10, (0, 10), (3, 7)),
        ])
        self.assertEqual(got[0]["span"], (0, 10))
        # 整组切片：副本位覆盖整条记录
        got = ds.slice(0, 10)
        self.assertEqual(got[0]["replicas"], (0, 10))


class ValidationTest(DatasetTestCase):
    def assert_bad(self, rows, sender="x"):
        ds = Dataset.create(self.db("v.db"), overwrite=True)
        self.addCleanup(ds.close)
        with self.assertRaises(ShardError):
            ds.declare_shard(sender, rows)

    def test_out_of_order_rejected(self):
        self.assert_bad([(1, 5, 1), (2, 3, 1)])
        self.assert_bad([(1, None, 1), (2, 3, 1)])  # 未知值必须居末

    def test_illegal_weight_rejected(self):
        self.assert_bad([(1, 5, 0)])
        self.assert_bad([(1, 5, -2)])
        self.assert_bad([(1, 5, 2.5)])
        self.assert_bad([(1, 5, True)])
        self.assert_bad([(1, 5, "abc")])

    def test_illegal_value_rejected(self):
        self.assert_bad([(1, 2 ** 63, 1)])
        self.assert_bad([(1, -(2 ** 63) - 1, 1)])
        self.assert_bad([(1, 1.5, 1)])
        self.assert_bad([(1, "7", 1)])

    def test_duplicate_record_no_rejected(self):
        self.assert_bad([(1, 5, 1), (1, 6, 1)])

    def test_bad_sender_rejected(self):
        self.assert_bad([(1, 5, 1)], sender="")
        self.assert_bad([(1, 5, 1)], sender=123)

    def test_weight_string_accepted(self):
        ds = Dataset.create(self.db("w.db"),
                            [("s", [(1, 1, str(2 ** 70))])])
        self.addCleanup(ds.close)
        self.assertEqual(ds.rank_interval(1)["weight"], 2 ** 70)

    def test_shard_limit(self):
        shards = [(f"shard_{i:02d}", [(1, i, 1)]) for i in range(64)]
        ds = Dataset.create(self.db("many.db"), shards)
        self.addCleanup(ds.close)
        self.assertEqual(ds.shard_count(), 64)
        with self.assertRaises(ShardError):
            ds.declare_shard("shard_64", [(1, 64, 1)])


class ReopenTest(DatasetTestCase):
    """新进程不访问输入分片也能查询；数据集可重开。"""

    def test_reopen_without_input_shards(self):
        fx_dir = FIXTURES / "unknown_tail"
        copied = self.tmp / "inputs"
        shutil.copytree(fx_dir, copied)
        shards = [load_shard(p) for p in sorted(copied.glob("*.json"))]
        path = self.db()
        ds = Dataset.create(path, shards)
        ds.close()
        shutil.rmtree(copied)  # 输入分片已不可用

        ds2 = Dataset(path)  # 模拟新进程重开
        self.addCleanup(ds2.close)
        self.assertEqual(ds2.total_weight(), 11)
        self.assertEqual((ds2.rank_interval(None)["start"],
                          ds2.rank_interval(None)["weight"]), (5, 6))
        self.assertEqual(as_tuples(ds2.slice(4, 2)), [
            ("south", 9, 1, 4, 2, (3, 5), (1, 2)),
            ("north", 2, 1, None, 3, (5, 11), (0, 1)),
        ])
        # 重开后重复声明仍幂等
        self.assertEqual(ds2.declare_shard(*shards[0]), "duplicate")
        self.assertEqual(ds2.shard_count(), 2)
        ds2.close()

        # 重开后仍可更正
        ds3 = Dataset(path)
        self.addCleanup(ds3.close)
        self.assertEqual(ds3.correct_shard("north", [(1, -5, 1)]), "corrected")
        self.assertEqual(ds3.total_weight(), 6)  # 11 - 6(旧north) + 1(新north)


class PageBoundaryTest(DatasetTestCase):
    def setUp(self):
        super().setUp()
        self.ds = Dataset.create(self.db(), [
            ("a", [(1, 1, 2), (2, 2, 1)]),
            ("b", [(1, 1, 3), (2, 5, 2)]),
        ])
        self.addCleanup(self.ds.close)

    def test_boundaries(self):
        ds = self.ds
        self.assertEqual(ds.slice(0, 0), [])
        self.assertEqual(ds.slice(8, 1), [])   # 位置 == 总量
        self.assertEqual(ds.slice(99, 3), [])  # 位置超出总量
        with self.assertRaises(ValueError):
            ds.slice(-1, 2)
        with self.assertRaises(ValueError):
            ds.slice(0, -1)
        # 恰好覆盖一个并列组
        self.assertEqual(as_tuples(ds.slice(0, 5)), [
            ("a", 1, 0, 1, 2, (0, 5), (0, 2)),
            ("b", 1, 0, 1, 3, (0, 5), (0, 3)),
        ])
        # 空数据集
        empty = Dataset.create(self.db("empty.db"))
        self.addCleanup(empty.close)
        self.assertEqual(empty.slice(0, 5), [])
        self.assertEqual(empty.rank_interval(1)["insertion"], 0)
        self.assertEqual(empty.rank_interval(None)["insertion"], 0)


if __name__ == "__main__":
    unittest.main()
