"""两期估价分布的迁移台账。

输入：两份已关账的 Dataset SQLite 文件（旧期、新期，只读打开）与一份
JSONL 转列清单（可以为空、可以无序）；交付：一个新的 SQLite 台账文件。

清单每行一条声明（JSON 对象）：

    {"old": {"sender": "...", "record_no": 1, "start": "0"},
     "new": {"sender": "...", "record_no": 2, "start": "5"},
     "length": "3"}

副本位从零起，是记录内位置（不是全库秩）；start/length 为十进制整数字符串，
length 必须为正。声明把旧端半开区间 [start, start+length) 与新端同长区间
按相对顺序一一对应，两端可以是同一地区或不同地区。

规则：
- 端点记录必须存在，区间不得越过该记录权重；
- 同一期同一记录内，任意两条声明不得占用同一副本位（重复声明也算冲突，
  区间首尾相接合法）；
- 未被声明占用的位置才可默认承接：两期都有同一 (sender, record_no) 且同一
  副本位在两端都空闲时配成一对；两端余量不压紧再配，也不按估值猜测去向；
- 旧端余量记退出，新端余量记新增；
- 任一行出错（坏行、找不到端点、越界、重叠）整份台账不交付，
  错误指出相应的清单行。

交付文件先建在临时路径，完成后原子链接到目标路径：半成品不会出现在目标
路径，已有目标不覆盖。全程按区间处理，不逐副本展开；权重、副本位与地区
合计可超有符号 64 位，以十进制文本落盘，在 Python 大整数上运算。声明与
记录都流式处理：内存占用与声明总数、单条记录的声明数无关。
"""

from __future__ import annotations

import hashlib
import heapq
import json
import os
import re
import secrets
import sqlite3
from urllib.parse import quote

from .dataset import MAX_RECORD_NO, SCHEMA_VERSION as _DATASET_SCHEMA_VERSION

__all__ = ["build_ledger", "Ledger", "LedgerError", "LEDGER_SCHEMA_VERSION"]

LEDGER_SCHEMA_VERSION = "1"

_DECIMAL_RE = re.compile(r"[0-9]+")

# 明细类别：transfer 声明转列 / carry 默认承接 / exit 退出 / new 新增
_KINDS = ("transfer", "carry", "exit", "new")

_LEDGER_SCHEMA = """
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
-- 两期记录登记：period 0=旧期 1=新期；weight 为十进制正整数文本(可超 int64)
CREATE TABLE records (
  period     INTEGER NOT NULL CHECK (period IN (0, 1)),
  sender     TEXT NOT NULL,
  record_no  INTEGER NOT NULL,
  is_unknown INTEGER NOT NULL CHECK (is_unknown IN (0, 1)),
  valuation  INTEGER,                -- NULL 当且仅当估值未知
  weight     TEXT NOT NULL,
  PRIMARY KEY (period, sender, record_no),
  CHECK ((is_unknown = 1) = (valuation IS NULL)),
  CHECK (weight GLOB '[1-9]*' AND weight NOT GLOB '*[^0-9]*')
);
-- 迁移明细（以区间为单位）：
--   transfer 声明转列（line_no 可回查清单行，1 起）
--   carry    默认承接（同一 (sender, record_no) 同一副本位：old_start = new_start）
--   exit     退出（只有旧端）  new 新增（只有新端）
-- 缺少的一端整组列为 NULL；估值未知与缺端分开：未知是 is_unknown=1 且
-- valuation IS NULL，但 sender/record_no/start 仍在。
CREATE TABLE entries (
  entry_id       INTEGER PRIMARY KEY,
  kind           TEXT NOT NULL CHECK (kind IN ('transfer','carry','exit','new')),
  line_no        INTEGER,            -- 清单行号(1 起)，仅 transfer
  old_sender     TEXT,
  old_record_no  INTEGER,
  old_is_unknown INTEGER,
  old_valuation  INTEGER,
  old_start      TEXT,               -- 十进制副本位(可超 int64)
  new_sender     TEXT,
  new_record_no  INTEGER,
  new_is_unknown INTEGER,
  new_valuation  INTEGER,
  new_start      TEXT,
  length         TEXT NOT NULL,      -- 共同长度，十进制正整数
  CHECK (length GLOB '[1-9]*' AND length NOT GLOB '*[^0-9]*'),
  CHECK ((old_sender IS NULL) = (old_record_no IS NULL)),
  CHECK ((old_sender IS NULL) = (old_start IS NULL)),
  CHECK ((old_sender IS NULL) = (old_is_unknown IS NULL)),
  CHECK (old_sender IS NULL OR (old_is_unknown IN (0, 1)
         AND (old_is_unknown = 1) = (old_valuation IS NULL))),
  CHECK ((new_sender IS NULL) = (new_record_no IS NULL)),
  CHECK ((new_sender IS NULL) = (new_start IS NULL)),
  CHECK ((new_sender IS NULL) = (new_is_unknown IS NULL)),
  CHECK (new_sender IS NULL OR (new_is_unknown IN (0, 1)
         AND (new_is_unknown = 1) = (new_valuation IS NULL))),
  CHECK (old_start IS NULL OR old_start = '0'
         OR (old_start GLOB '[1-9]*' AND old_start NOT GLOB '*[^0-9]*')),
  CHECK (new_start IS NULL OR new_start = '0'
         OR (new_start GLOB '[1-9]*' AND new_start NOT GLOB '*[^0-9]*')),
  CHECK (CASE kind
    WHEN 'transfer' THEN line_no IS NOT NULL
                         AND old_sender IS NOT NULL AND new_sender IS NOT NULL
    WHEN 'carry'    THEN line_no IS NULL
                         AND old_sender IS NOT NULL AND new_sender IS NOT NULL
                         AND old_sender = new_sender
                         AND old_record_no = new_record_no
                         AND old_start = new_start
    WHEN 'exit'     THEN line_no IS NULL
                         AND old_sender IS NOT NULL AND new_sender IS NULL
    WHEN 'new'      THEN line_no IS NULL
                         AND old_sender IS NULL AND new_sender IS NOT NULL
  END)
);
-- 大整数副本位按 (位数, 文本) 排序即数值序；供按记录有序核对与对账
CREATE INDEX entries_old_idx
  ON entries(old_sender, old_record_no, length(old_start), old_start);
CREATE INDEX entries_new_idx
  ON entries(new_sender, new_record_no, length(new_start), new_start);
-- 按旧、新地区组合的精确合计重量；退出缺新地区、新增缺旧地区
CREATE TABLE region_totals (
  old_sender TEXT,
  new_sender TEXT,
  weight     TEXT NOT NULL
             CHECK (weight GLOB '[1-9]*' AND weight NOT GLOB '*[^0-9]*')
);
-- 分期的地区合计：按旧侧或新侧汇总分别还原该期各地区的总权重
CREATE TABLE sender_totals (
  period INTEGER NOT NULL CHECK (period IN (0, 1)),
  sender TEXT NOT NULL,
  weight TEXT NOT NULL
         CHECK (weight GLOB '[1-9]*' AND weight NOT GLOB '*[^0-9]*'),
  PRIMARY KEY (period, sender)
);
"""

# 清单占用登记（工作库，交付前删除）：start 用 (位数, 文本) 编码以支持
# 超 int64 的副本位做索引范围比较。
_CLAIMS_SCHEMA = """
CREATE TABLE claims (
  period          INTEGER NOT NULL CHECK (period IN (0, 1)),
  sender          TEXT NOT NULL,
  record_no       INTEGER NOT NULL,
  start_len       INTEGER NOT NULL,  -- start_txt 的位数
  start_txt       TEXT NOT NULL,     -- 起始副本位，十进制规范文本
  end_txt         TEXT NOT NULL,     -- start + length
  length_txt      TEXT NOT NULL,
  line_no         INTEGER NOT NULL,  -- 清单行号(1 起)
  peer_sender     TEXT NOT NULL,     -- 对端记录标识与估值（生成转列明细用）
  peer_record_no  INTEGER NOT NULL,
  peer_start_txt  TEXT NOT NULL,
  peer_is_unknown INTEGER NOT NULL,
  peer_valuation  INTEGER
);
CREATE INDEX claims_idx
  ON claims(period, sender, record_no, start_len, start_txt);
"""


class LedgerError(Exception):
    """清单验证失败、输入库无效或台账目标路径冲突。"""


def _is_int(x):
    return isinstance(x, int) and not isinstance(x, bool)


# ---------------------------------------------------------------- 清单解析

def _parse_endpoint(obj, line_no, tag):
    if not isinstance(obj, dict):
        raise LedgerError(f"清单第 {line_no} 行: {tag}端必须是对象")
    sender = obj.get("sender")
    if not isinstance(sender, str) or not sender:
        raise LedgerError(
            f"清单第 {line_no} 行: {tag}端 sender 必须是非空字符串")
    record_no = obj.get("record_no")
    if not _is_int(record_no) or record_no < 0 or record_no > MAX_RECORD_NO:
        raise LedgerError(
            f"清单第 {line_no} 行: {tag}端 record_no 必须是"
            f" [0, 2^63) 内的整数")
    start = obj.get("start")
    if not isinstance(start, str) or not _DECIMAL_RE.fullmatch(start):
        raise LedgerError(
            f"清单第 {line_no} 行: {tag}端 start 必须是十进制整数字符串")
    return sender, record_no, int(start)


def _parse_declaration(line, line_no):
    """解析一条声明，返回 ((旧端...), (新端...), length)；坏行抛 LedgerError。"""
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as exc:
        raise LedgerError(
            f"清单第 {line_no} 行: 不是有效的 JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise LedgerError(f"清单第 {line_no} 行: 声明必须是 JSON 对象")
    old = _parse_endpoint(obj.get("old"), line_no, "旧")
    new = _parse_endpoint(obj.get("new"), line_no, "新")
    length = obj.get("length")
    if not isinstance(length, str) or not _DECIMAL_RE.fullmatch(length):
        raise LedgerError(
            f"清单第 {line_no} 行: length 必须是十进制整数字符串")
    length = int(length)
    if length < 1:
        raise LedgerError(f"清单第 {line_no} 行: length 必须为正")
    return old, new, length


# ---------------------------------------------------------------- 输入库

def _open_dataset_ro(path, label):
    """只读打开一份已关账的数据集库并校验模式版本。"""
    if not os.path.exists(path):
        raise LedgerError(f"{label}数据集不存在: {path}")
    uri = "file:" + quote(os.path.abspath(path)) + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
        conn.isolation_level = None
        conn.execute("PRAGMA query_only = ON")
        row = conn.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error as exc:
        raise LedgerError(f"{label}数据集无法读取: {path}: {exc}") from exc
    if row is None or row[0] != _DATASET_SCHEMA_VERSION:
        conn.close()
        raise LedgerError(f"{label}不是有效的数据集库: {path}")
    return conn


def _lookup_record(conn, sender, record_no):
    """(weight:int, is_unknown, valuation)；记录不存在返回 None。"""
    row = conn.execute(
        "SELECT weight, is_unknown, valuation FROM rows"
        " WHERE sender = ? AND record_no = ?",
        (sender, record_no)).fetchone()
    if row is None:
        return None
    return int(row[0]), row[1], row[2]


# ------------------------------------------------------------ 清单占用登记

def _insert_claim(work, period, sender, record_no, start, end, length,
                  line_no, peer):
    """登记一个端点占用的区间；与已登记区间重叠则抛 LedgerError。

    已登记区间两两不相交，因此只需检查后继（start >= s 的第一条，其
    start < e 即重叠）与前驱（start < s 的最后一条，其 end > s 即重叠），
    区间首尾相接合法。两次索引定位，O(log n)，不攒全量声明。
    """
    side = "旧" if period == 0 else "新"
    s_txt = str(start)
    s_len = len(s_txt)
    row = work.execute(
        "SELECT start_txt, line_no FROM claims"
        " WHERE period = ? AND sender = ? AND record_no = ?"
        " AND (start_len, start_txt) >= (?, ?)"
        " ORDER BY start_len, start_txt LIMIT 1",
        (period, sender, record_no, s_len, s_txt)).fetchone()
    if row is not None and int(row[0]) < end:
        raise LedgerError(
            f"清单第 {line_no} 行: {side}端区间与清单第 {row[1]} 行声明"
            f"占用同一副本位")
    row = work.execute(
        "SELECT end_txt, line_no FROM claims"
        " WHERE period = ? AND sender = ? AND record_no = ?"
        " AND (start_len, start_txt) < (?, ?)"
        " ORDER BY start_len DESC, start_txt DESC LIMIT 1",
        (period, sender, record_no, s_len, s_txt)).fetchone()
    if row is not None and int(row[0]) > start:
        raise LedgerError(
            f"清单第 {line_no} 行: {side}端区间与清单第 {row[1]} 行声明"
            f"占用同一副本位")
    work.execute(
        "INSERT INTO claims(period, sender, record_no, start_len, start_txt,"
        " end_txt, length_txt, line_no, peer_sender, peer_record_no,"
        " peer_start_txt, peer_is_unknown, peer_valuation)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (period, sender, record_no, s_len, s_txt, str(end), str(length),
         line_no, peer[0], peer[1], str(peer[2]), peer[3], peer[4]))


def _add_declaration(work, old_conn, new_conn, decl, line_no):
    (o_sender, o_rec, o_start), (n_sender, n_rec, n_start), length = decl
    o_end = o_start + length
    n_end = n_start + length
    o_row = _lookup_record(old_conn, o_sender, o_rec)
    if o_row is None:
        raise LedgerError(
            f"清单第 {line_no} 行: 旧端记录不存在:"
            f" sender={o_sender!r} record_no={o_rec}")
    if o_end > o_row[0]:
        raise LedgerError(
            f"清单第 {line_no} 行: 旧端区间 [{o_start}, {o_end})"
            f" 越过记录权重 {o_row[0]}")
    n_row = _lookup_record(new_conn, n_sender, n_rec)
    if n_row is None:
        raise LedgerError(
            f"清单第 {line_no} 行: 新端记录不存在:"
            f" sender={n_sender!r} record_no={n_rec}")
    if n_end > n_row[0]:
        raise LedgerError(
            f"清单第 {line_no} 行: 新端区间 [{n_start}, {n_end})"
            f" 越过记录权重 {n_row[0]}")
    _insert_claim(work, 0, o_sender, o_rec, o_start, o_end, length, line_no,
                  (n_sender, n_rec, n_start, n_row[1], n_row[2]))
    _insert_claim(work, 1, n_sender, n_rec, n_start, n_end, length, line_no,
                  (o_sender, o_rec, o_start, o_row[1], o_row[2]))


# ------------------------------------------------------------ 区间流式代数

def _complement(spans, bound):
    """已占用区间流(有序、不相交)在 [0, bound) 内的空闲部分，O(1) 内存。"""
    pos = 0
    for s, e in spans:
        if s > pos:
            yield (pos, s)
        pos = e
    if pos < bound:
        yield (pos, bound)


def _split_free(ofree, nfree):
    """归并两侧空闲区间流：重叠 -> 'carry'，仅旧侧 -> 'exit'，仅新侧 -> 'new'。

    同一副本位两端都空闲才配成承接；余量不压紧再配。O(1) 内存。
    """
    a = next(ofree, None)
    b = next(nfree, None)
    while a is not None or b is not None:
        if a is None:
            yield ("new", b[0], b[1])
            b = next(nfree, None)
            continue
        if b is None:
            yield ("exit", a[0], a[1])
            a = next(ofree, None)
            continue
        if a[1] <= b[0]:
            yield ("exit", a[0], a[1])
            a = next(ofree, None)
            continue
        if b[1] <= a[0]:
            yield ("new", b[0], b[1])
            b = next(nfree, None)
            continue
        lo = a[0] if a[0] > b[0] else b[0]
        hi = a[1] if a[1] < b[1] else b[1]
        if a[0] < lo:
            yield ("exit", a[0], lo)
        if b[0] < lo:
            yield ("new", b[0], lo)
        yield ("carry", lo, hi)
        a = (hi, a[1]) if a[1] > hi else next(ofree, None)
        b = (hi, b[1]) if b[1] > hi else next(nfree, None)


# ------------------------------------------------------------ 台账写入

class _LedgerBuilder:
    """流式写入台账并累计地区合计；内存与记录数、声明数无关。"""

    def __init__(self, conn):
        self._conn = conn
        self.kinds = {k: 0 for k in _KINDS}
        self.entries = 0
        self.records = {0: 0, 1: 0}
        self.totals = {0: 0, 1: 0}          # 明细长度按侧累计
        self.record_weights = {0: 0, 1: 0}  # 记录权重按侧累计
        self.region = {}                    # (旧地区|None, 新地区|None) -> 重量
        self.sender_totals = {0: {}, 1: {}}

    def add_record(self, period, row):
        sender, record_no, is_unknown, valuation, weight_txt = row
        self._conn.execute(
            "INSERT INTO records(period, sender, record_no, is_unknown,"
            " valuation, weight) VALUES (?, ?, ?, ?, ?, ?)",
            (period, sender, record_no, is_unknown, valuation, weight_txt))
        self.records[period] += 1
        self.record_weights[period] += int(weight_txt)

    def add_entry(self, kind, line_no, old, old_start, new, new_start,
                  length):
        """old/new: (sender, record_no, is_unknown, valuation) 或 None。"""
        self._conn.execute(
            "INSERT INTO entries(kind, line_no, old_sender, old_record_no,"
            " old_is_unknown, old_valuation, old_start,"
            " new_sender, new_record_no, new_is_unknown, new_valuation,"
            " new_start, length)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (kind, line_no,
             old[0] if old else None, old[1] if old else None,
             old[2] if old else None, old[3] if old else None,
             str(old_start) if old is not None else None,
             new[0] if new else None, new[1] if new else None,
             new[2] if new else None, new[3] if new else None,
             str(new_start) if new is not None else None,
             str(length)))
        self.kinds[kind] += 1
        self.entries += 1
        o_key = old[0] if old else None
        n_key = new[0] if new else None
        rk = (o_key, n_key)
        self.region[rk] = self.region.get(rk, 0) + length
        if old is not None:
            self.totals[0] += length
            st = self.sender_totals[0]
            st[o_key] = st.get(o_key, 0) + length
        if new is not None:
            self.totals[1] += length
            st = self.sender_totals[1]
            st[n_key] = st.get(n_key, 0) + length


def _claim_spans(work, period, sender, record_no):
    """该记录该期已占用区间流（按起始位有序），O(1) 内存。"""
    cur = work.execute(
        "SELECT start_txt, end_txt FROM claims"
        " WHERE period = ? AND sender = ? AND record_no = ?"
        " ORDER BY start_len, start_txt", (period, sender, record_no))
    for s_txt, e_txt in cur:
        yield (int(s_txt), int(e_txt))


def _transfer_claims(work, sender, record_no):
    """该记录旧端被声明占用的行（含新端信息），按旧端起始位有序。"""
    return work.execute(
        "SELECT start_txt, length_txt, line_no, peer_sender, peer_record_no,"
        " peer_start_txt, peer_is_unknown, peer_valuation FROM claims"
        " WHERE period = 0 AND sender = ? AND record_no = ?"
        " ORDER BY start_len, start_txt", (sender, record_no))


def _emit_record(led, work, o, n):
    """处理一个 (sender, record_no) 键：o/n 为两期记录行或 None。"""
    sender, record_no = (o if o is not None else n)[0:2]
    if o is not None:
        led.add_record(0, o)
    if n is not None:
        led.add_record(1, n)
    w_old = int(o[4]) if o is not None else 0
    w_new = int(n[4]) if n is not None else 0

    def ofree():
        if o is None:
            return iter(())
        return _complement(_claim_spans(work, 0, sender, record_no), w_old)

    def nfree():
        if n is None:
            return iter(())
        return _complement(_claim_spans(work, 1, sender, record_no), w_new)

    # 第一遍：旧侧平铺——声明转列与承接/退出按旧侧副本位归并
    gap_entries = ((s, tag, e) for tag, s, e in _split_free(ofree(), nfree())
                   if tag != "new")
    streams = [gap_entries]
    if o is not None:
        streams.append(
            (int(r[0]), "transfer", r)
            for r in _transfer_claims(work, sender, record_no))
    for pos, kind, payload in heapq.merge(*streams, key=lambda item: item[0]):
        if kind == "transfer":
            (_s, length_txt, line_no, p_sender, p_rec, p_start,
             p_unk, p_val) = payload
            led.add_entry("transfer", line_no, o[:4], pos,
                          (p_sender, p_rec, p_unk, p_val), int(p_start),
                          int(length_txt))
        elif kind == "carry":
            led.add_entry("carry", None, o[:4], pos, n[:4], pos,
                          payload - pos)
        else:  # exit
            led.add_entry("exit", None, o[:4], pos, None, None,
                          payload - pos)
    # 第二遍：新侧余量记新增（区间流重新取，不攒内存）
    for tag, s, e in _split_free(ofree(), nfree()):
        if tag == "new":
            led.add_entry("new", None, None, None, n[:4], s, e - s)


def _emit_all(led, work, old_conn, new_conn):
    """归并两期记录流（都按 (sender, record_no) 有序），逐键流式生成明细。"""
    old_cur = old_conn.execute(
        "SELECT sender, record_no, is_unknown, valuation, weight FROM rows"
        " ORDER BY sender, record_no")
    new_cur = new_conn.execute(
        "SELECT sender, record_no, is_unknown, valuation, weight FROM rows"
        " ORDER BY sender, record_no")
    o = next(old_cur, None)
    n = next(new_cur, None)
    while o is not None or n is not None:
        if o is not None and n is not None and o[0] == n[0] and o[1] == n[1]:
            _emit_record(led, work, o, n)
            o = next(old_cur, None)
            n = next(new_cur, None)
        elif n is None or (o is not None and (o[0], o[1]) < (n[0], n[1])):
            _emit_record(led, work, o, None)
            o = next(old_cur, None)
        else:
            _emit_record(led, work, None, n)
            n = next(new_cur, None)


def _finalize(led, conn, manifest_lines, manifest_sha):
    # 不变量：每条记录的每个副本位恰好落到一条明细上
    for period in (0, 1):
        if led.totals[period] != led.record_weights[period]:
            raise LedgerError("内部错误: 明细合计与记录权重不符")
    for (o_s, n_s), w in sorted(
            led.region.items(),
            key=lambda kv: (kv[0][0] is None, kv[0][0] or "",
                            kv[0][1] is None, kv[0][1] or "")):
        conn.execute(
            "INSERT INTO region_totals(old_sender, new_sender, weight)"
            " VALUES (?, ?, ?)", (o_s, n_s, str(w)))
    for period in (0, 1):
        for sender, w in sorted(led.sender_totals[period].items()):
            conn.execute(
                "INSERT INTO sender_totals(period, sender, weight)"
                " VALUES (?, ?, ?)", (period, sender, str(w)))
    meta = {
        "ledger_version": LEDGER_SCHEMA_VERSION,
        "entries_total": str(led.entries),
        "entries_transfer": str(led.kinds["transfer"]),
        "entries_carry": str(led.kinds["carry"]),
        "entries_exit": str(led.kinds["exit"]),
        "entries_new": str(led.kinds["new"]),
        "records_old": str(led.records[0]),
        "records_new": str(led.records[1]),
        "total_old_weight": str(led.totals[0]),
        "total_new_weight": str(led.totals[1]),
        "manifest_lines": str(manifest_lines),
        "manifest_sha256": manifest_sha,
    }
    for k, v in meta.items():
        conn.execute("INSERT INTO meta(key, value) VALUES (?, ?)", (k, v))


# ------------------------------------------------------------ 生成入口

def build_ledger(old_path, new_path, manifest_path, out_path):
    """生成两期迁移台账。

    参数：
      old_path/new_path  两份已关账的 Dataset SQLite 文件（只读打开）
      manifest_path      JSONL 转列清单（可以为空、可以无序）
      out_path           台账交付路径；已存在则拒绝覆盖

    任一清单行出错（坏行、找不到端点、越界、重叠）整份台账不交付，
    错误指出清单行号；半成品不会出现在目标路径。

    返回摘要 dict：entries/kinds/declarations/records/total_weight。
    """
    old_path = str(old_path)
    new_path = str(new_path)
    manifest_path = str(manifest_path)
    out_path = str(out_path)
    if os.path.exists(out_path):
        raise LedgerError(f"目标已存在: {out_path}")
    out_dir = os.path.dirname(os.path.abspath(out_path))
    if not os.path.isdir(out_dir):
        raise LedgerError(f"目标目录不存在: {out_dir}")
    if not os.path.isfile(manifest_path):
        raise LedgerError(f"转列清单不存在: {manifest_path}")
    old_conn = _open_dataset_ro(old_path, "旧期")
    try:
        new_conn = _open_dataset_ro(new_path, "新期")
    except BaseException:
        old_conn.close()
        raise
    token = f"{os.getpid()}-{secrets.token_hex(4)}"
    base = os.path.basename(out_path)
    work_path = os.path.join(out_dir, f".{base}.claims-{token}.tmp")
    tmp_path = os.path.join(out_dir, f".{base}.ledger-{token}.tmp")
    try:
        work = sqlite3.connect(work_path)
        work.isolation_level = None
        work.execute("PRAGMA journal_mode = OFF")   # 纯暂存，交付前删除
        work.execute("PRAGMA synchronous = OFF")
        work.executescript(_CLAIMS_SCHEMA)
        # 1) 流式读清单：验证端点、登记占用；重叠即报错并指出清单行
        sha = hashlib.sha256()
        n_decl = 0
        try:
            fh = open(manifest_path, "r", encoding="utf-8", newline="")
        except OSError as exc:
            raise LedgerError(
                f"转列清单无法读取: {manifest_path}: {exc}") from exc
        with fh:
            try:
                for line_no, line in enumerate(fh, 1):
                    sha.update(line.encode("utf-8"))
                    if not line.strip():
                        continue
                    decl = _parse_declaration(line, line_no)
                    _add_declaration(work, old_conn, new_conn, decl, line_no)
                    n_decl += 1
            except UnicodeDecodeError as exc:
                raise LedgerError(
                    f"转列清单不是有效的 UTF-8: {manifest_path}") from exc
        # 2) 归并两期记录，流式生成交付明细与合计
        led_conn = sqlite3.connect(tmp_path)
        led_conn.isolation_level = None
        led_conn.executescript(_LEDGER_SCHEMA)
        led = _LedgerBuilder(led_conn)
        led_conn.execute("BEGIN")
        try:
            _emit_all(led, work, old_conn, new_conn)
            _finalize(led, led_conn, n_decl, sha.hexdigest())
            led_conn.execute("COMMIT")
        except BaseException:
            led_conn.execute("ROLLBACK")
            raise
        led_conn.close()
        work.close()
        # 3) 落盘后原子链接到目标；目标已存在（含并发出现）则不覆盖
        fd = os.open(tmp_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        os.link(tmp_path, out_path)
        return {
            "out": out_path,
            "entries": led.entries,
            "kinds": dict(led.kinds),
            "declarations": n_decl,
            "records": {"old": led.records[0], "new": led.records[1]},
            "total_weight": {"old": led.totals[0], "new": led.totals[1]},
        }
    except FileExistsError:
        raise LedgerError(f"目标已存在: {out_path}") from None
    finally:
        old_conn.close()
        new_conn.close()
        for p in (tmp_path, work_path):
            try:
                os.unlink(p)
            except OSError:
                pass


# ------------------------------------------------------------ 台账只读访问

class Ledger:
    """迁移台账（build_ledger 的交付物）的只读访问与独立核对。

    核对只读本台账文件，不需要打开输入数据集与转列清单。
    """

    _ENTRY_COLS = ("entry_id", "kind", "line_no",
                   "old_sender", "old_record_no", "old_is_unknown",
                   "old_valuation", "old_start",
                   "new_sender", "new_record_no", "new_is_unknown",
                   "new_valuation", "new_start", "length")

    def __init__(self, path):
        self._path = str(path)
        if not os.path.exists(self._path):
            raise LedgerError(f"台账不存在: {self._path}")
        uri = "file:" + quote(os.path.abspath(self._path)) + "?mode=ro"
        try:
            self._conn = sqlite3.connect(uri, uri=True)
        except sqlite3.Error as exc:
            raise LedgerError(f"台账无法读取: {self._path}: {exc}") from exc
        self._conn.isolation_level = None
        self._conn.execute("PRAGMA query_only = ON")
        try:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'ledger_version'"
            ).fetchone()
        except sqlite3.Error as exc:
            self._conn.close()
            raise LedgerError(f"不是有效的台账库: {self._path}") from exc
        if row is None or row[0] != LEDGER_SCHEMA_VERSION:
            self._conn.close()
            raise LedgerError(f"台账模式版本不兼容: {self._path}")

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    # ---------------------------------------------------------- 查询

    def meta(self):
        return {k: v for k, v in
                self._conn.execute("SELECT key, value FROM meta")}

    @staticmethod
    def _entry_dict(row):
        d = dict(zip(Ledger._ENTRY_COLS, row))

        def side(p):
            if d[p + "_sender"] is None:
                return None
            return {"sender": d[p + "_sender"],
                    "record_no": d[p + "_record_no"],
                    "is_unknown": bool(d[p + "_is_unknown"]),
                    "value": d[p + "_valuation"],
                    "start": int(d[p + "_start"])}

        return {"entry_id": d["entry_id"], "kind": d["kind"],
                "line_no": d["line_no"], "old": side("old"),
                "new": side("new"), "length": int(d["length"])}

    def entries(self, kind=None):
        """按 entry_id 序流式返回明细；kind 可限定 transfer/carry/exit/new。"""
        sql = ("SELECT entry_id, kind, line_no, old_sender, old_record_no,"
               " old_is_unknown, old_valuation, old_start, new_sender,"
               " new_record_no, new_is_unknown, new_valuation, new_start,"
               " length FROM entries")
        if kind is None:
            cur = self._conn.execute(sql + " ORDER BY entry_id")
        else:
            if kind not in _KINDS:
                raise ValueError(f"未知明细类别: {kind!r}")
            cur = self._conn.execute(
                sql + " WHERE kind = ? ORDER BY entry_id", (kind,))
        for row in cur:
            yield self._entry_dict(row)

    def records(self, period):
        """某一期（0=旧 1=新）的记录登记，按 (sender, record_no) 序。"""
        if period not in (0, 1):
            raise ValueError(f"period 必须是 0(旧期) 或 1(新期): {period!r}")
        for sender, rec, unk, val, w in self._conn.execute(
                "SELECT sender, record_no, is_unknown, valuation, weight"
                " FROM records WHERE period = ? ORDER BY sender, record_no",
                (period,)):
            yield {"sender": sender, "record_no": rec,
                   "is_unknown": bool(unk), "value": val, "weight": int(w)}

    def region_totals(self):
        """按 (旧地区, 新地区) 组合的精确合计；退出缺新地区、新增缺旧地区。"""
        return [{"old_sender": o, "new_sender": n, "weight": int(w)}
                for o, n, w in self._conn.execute(
                    "SELECT old_sender, new_sender, weight FROM region_totals"
                    " ORDER BY old_sender, new_sender")]

    def sender_totals(self, period):
        """某一期各地区的总权重：{sender: int}。"""
        if period not in (0, 1):
            raise ValueError(f"period 必须是 0(旧期) 或 1(新期): {period!r}")
        return {s: int(w) for s, w in self._conn.execute(
            "SELECT sender, weight FROM sender_totals WHERE period = ?"
            " ORDER BY sender", (period,))}

    # ---------------------------------------------------------- 独立核对

    def verify(self):
        """不打开输入库与清单，独立核对台账内部一致性。

        核对：明细结构、转列行号唯一、每条记录两侧明细恰好铺满其权重、
        地区组合合计 == 明细聚合、分期地区合计 == 组合合计按侧汇总 ==
        记录登记按侧汇总、meta 统计。全部通过返回摘要，否则抛 LedgerError。
        """
        conn = self._conn
        # 1) 明细结构（与库内 CHECK 独立复核）
        bad = conn.execute(
            "SELECT COUNT(*) FROM entries WHERE"
            " (kind = 'transfer' AND (line_no IS NULL OR old_sender IS NULL"
            "  OR new_sender IS NULL))"
            " OR (kind = 'carry' AND (line_no IS NOT NULL OR old_sender IS NULL"
            "  OR new_sender IS NULL OR old_sender <> new_sender"
            "  OR old_record_no <> new_record_no OR old_start <> new_start))"
            " OR (kind = 'exit' AND (line_no IS NOT NULL OR old_sender IS NULL"
            "  OR new_sender IS NOT NULL))"
            " OR (kind = 'new' AND (line_no IS NOT NULL OR old_sender IS NOT NULL"
            "  OR new_sender IS NULL))").fetchone()[0]
        if bad:
            raise LedgerError(f"台账明细结构违规: {bad} 行")
        n_tr, n_tr_distinct = conn.execute(
            "SELECT COUNT(*), COUNT(DISTINCT line_no) FROM entries"
            " WHERE kind = 'transfer'").fetchone()
        if n_tr != n_tr_distinct:
            raise LedgerError("台账转列明细的清单行号不唯一")
        # 2) 两侧平铺：每条记录的明细恰好铺满 [0, weight)
        n_records = {}
        for period in (0, 1):
            n_records[period] = self._verify_tiling(period)
        # 3) region_totals == 明细聚合
        agg = {}
        kinds = {k: 0 for k in _KINDS}
        totals = {0: 0, 1: 0}
        n_entries = 0
        for kind, o_s, n_s, length_txt in conn.execute(
                "SELECT kind, old_sender, new_sender, length FROM entries"):
            length = int(length_txt)
            key = (o_s, n_s)
            agg[key] = agg.get(key, 0) + length
            kinds[kind] += 1
            n_entries += 1
            if o_s is not None:
                totals[0] += length
            if n_s is not None:
                totals[1] += length
        table = {}
        for o_s, n_s, w in conn.execute(
                "SELECT old_sender, new_sender, weight FROM region_totals"):
            key = (o_s, n_s)
            if key in table:
                raise LedgerError(f"地区组合合计重复: {key}")
            table[key] = int(w)
        if agg != table:
            raise LedgerError("地区组合合计与明细聚合不符")
        # 4) sender_totals == 组合合计按侧汇总 == 记录登记按侧汇总
        rec_totals = {0: {}, 1: {}}
        for period in (0, 1):
            for sender, w in conn.execute(
                    "SELECT sender, weight FROM records WHERE period = ?",
                    (period,)):
                rt = rec_totals[period]
                rt[sender] = rt.get(sender, 0) + int(w)
        for period, idx in ((0, 0), (1, 1)):
            from_region = {}
            for key, w in agg.items():
                s = key[idx]
                if s is not None:
                    from_region[s] = from_region.get(s, 0) + w
            if from_region != self.sender_totals(period):
                raise LedgerError(
                    f"{'旧' if period == 0 else '新'}侧地区合计与"
                    f"地区组合合计不符")
            if rec_totals[period] != self.sender_totals(period):
                raise LedgerError(
                    f"{'旧' if period == 0 else '新'}侧地区合计与"
                    f"记录登记不符")
        # 5) meta 统计
        meta = self.meta()

        def m(key):
            try:
                return int(meta[key])
            except (KeyError, ValueError):
                raise LedgerError(f"台账 meta 缺少或非法: {key}")

        if m("entries_total") != n_entries:
            raise LedgerError("meta entries_total 与明细不符")
        for k in _KINDS:
            if m("entries_" + k) != kinds[k]:
                raise LedgerError(f"meta entries_{k} 与明细不符")
        if (m("total_old_weight"), m("total_new_weight")) != \
                (totals[0], totals[1]):
            raise LedgerError("meta 总权重与明细不符")
        if (m("records_old"), m("records_new")) != \
                (n_records[0], n_records[1]):
            raise LedgerError("meta 记录数与登记不符")
        if m("manifest_lines") != kinds["transfer"]:
            raise LedgerError("meta 清单行数与转列明细不符")
        return {
            "entries": n_entries,
            "kinds": kinds,
            "records": {"old": n_records[0], "new": n_records[1]},
            "total_weight": {"old": totals[0], "new": totals[1]},
            "region_totals": len(table),
        }

    def _verify_tiling(self, period):
        """该期每条记录的明细按副本位恰好铺满 [0, weight)；返回记录数。"""
        conn = self._conn
        if period == 0:
            sql = ("SELECT old_sender, old_record_no, old_start, length"
                   " FROM entries WHERE old_sender IS NOT NULL"
                   " ORDER BY old_sender, old_record_no,"
                   " length(old_start), old_start")
            label = "旧"
        else:
            sql = ("SELECT new_sender, new_record_no, new_start, length"
                   " FROM entries WHERE new_sender IS NOT NULL"
                   " ORDER BY new_sender, new_record_no,"
                   " length(new_start), new_start")
            label = "新"
        n_groups = 0
        cur_key = None
        pos = 0
        for sender, rec, start_txt, length_txt in conn.execute(sql):
            key = (sender, rec)
            if key != cur_key:
                if cur_key is not None:
                    self._check_record_weight(period, cur_key, pos, label)
                cur_key = key
                pos = 0
                n_groups += 1
            start = int(start_txt)
            if str(start) != start_txt:
                raise LedgerError(f"台账副本位非规范十进制: {start_txt!r}")
            length = int(length_txt)
            if length < 1:
                raise LedgerError(f"台账明细长度非正: {length_txt!r}")
            if start != pos:
                raise LedgerError(
                    f"台账{label}侧记录 {key} 的明细在副本位 {pos}"
                    f" 处断裂或重叠")
            pos = start + length
        if cur_key is not None:
            self._check_record_weight(period, cur_key, pos, label)
        n_records = conn.execute(
            "SELECT COUNT(*) FROM records WHERE period = ?",
            (period,)).fetchone()[0]
        if n_groups != n_records:
            raise LedgerError(f"台账{label}侧记录登记与明细不符")
        return n_records

    def _check_record_weight(self, period, key, pos, label):
        row = self._conn.execute(
            "SELECT weight FROM records"
            " WHERE period = ? AND sender = ? AND record_no = ?",
            (period, key[0], key[1])).fetchone()
        if row is None:
            raise LedgerError(f"台账明细引用了未登记的记录: {key}")
        if pos != int(row[0]):
            raise LedgerError(
                f"台账{label}侧记录 {key} 的明细合计 {pos}"
                f" 与记录权重 {row[0]} 不符")
