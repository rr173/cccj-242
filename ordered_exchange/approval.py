"""审批合并：把原批件与左、右两份修订稿台账合并成一份审批结果。

三位核算员从同一份转列批件出发：原批件台账与两份各自试算成功的修订稿台账
（均为 build_ledger 的交付物）。本入口只读这三份台账，不访问原始数据集与
转列清单，产出两种互斥结果之一：

- 可共同出账：交付一份能直接传给 build_ledger 的合并转列清单（JSONL）；
- 待协调：交付一份完整争议明细（JSONL），不交付任何清单（连部分清单也不
  交付）。

合并对象只是已明确申报的 transfer 对应关系：carry/exit/new 不是审批指令，
合并清单投入核算后，这些非声明明细仍由原有核算规则确定。

三份台账的旧端记录登记必须逐条相同，新端登记也必须逐条相同（记录标识、
权重、估值；未知估值只与未知相同）。登记不符时指出哪份稿、哪一端、哪条
记录有差异，不交付任何文件。

判定以旧端的每个副本位为单位，比较三稿在该位的指向（新端记录标识与副本
位；未声明则没有指向）：

- 双稿指向一致（含都未声明、都撤回）就采用该指向；
- 仅一稿与原批件不同时采用变更稿，包括撤回；
- 两稿相对原批件都变了且指向不同（含一方撤回、另一方改派）即旧端争议。

没有旧端争议的指向还要在新端相容：同一新端副本位被两个或更多旧端副本位
占用即新端争议（同一旧端记录的不同位置也算不同占用者）。只用已经判定的
指向检查，旧端争议位不参与，也不猜测其去向；旧端争议不掩盖其他已能确定
的新端争议。

争议段相接时合段，当且仅当每个选项（旧端争议）或占用者（新端争议）在另
一端的记录不变且各自区间顺次相接；没有指向的选项只与同样没有指向的相邻
选项合段。所有区间为半开区间，起点非负、长度为正，起点与长度一律用十进
制字符串；副本位可超过有符号 64 位整数。

全程按区间流式处理：不逐副本展开，不在内存保留任一整稿、某条热点记录的
全部映射/争议/占用；中间状态落在临时 SQLite 工作库，交付前删除。
"""

from __future__ import annotations

import heapq
import json
import os
import secrets
import sqlite3
from urllib.parse import quote

from .ledger import LEDGER_SCHEMA_VERSION

__all__ = ["merge_approvals", "ApprovalError", "STATUS_APPROVED",
           "STATUS_DISPUTED"]

STATUS_APPROVED = "可出账"    # 无争议：交付合并清单
STATUS_DISPUTED = "待协调"    # 有争议：交付争议明细

# 工作库：只放已判定（无旧端争议）的指向段，按区间存储，支持两侧索引扫描。
# 起止位同时存位数与文本：按 (位数, 文本) 排序即数值序，支持超 int64 副本位。
_WORK_SCHEMA = """
CREATE TABLE resolved (
  o_sender     TEXT NOT NULL,
  o_record_no  INTEGER NOT NULL,
  o_start_len  INTEGER NOT NULL,
  o_start_txt  TEXT NOT NULL,
  o_end_len    INTEGER NOT NULL,
  o_end_txt    TEXT NOT NULL,
  n_sender     TEXT NOT NULL,
  n_record_no  INTEGER NOT NULL,
  n_start_len  INTEGER NOT NULL,
  n_start_txt  TEXT NOT NULL,
  n_end_len    INTEGER NOT NULL,
  n_end_txt    TEXT NOT NULL,
  length_txt   TEXT NOT NULL
);
CREATE INDEX res_old_idx
  ON resolved(o_sender, o_record_no, o_start_len, o_start_txt);
CREATE INDEX res_new_start_idx
  ON resolved(n_sender, n_record_no, n_start_len, n_start_txt);
"""

_LEDGER_LABELS = ("原批件台账", "左稿台账", "右稿台账")
_SIDE_LABELS = {0: "旧端", 1: "新端"}


class ApprovalError(Exception):
    """台账登记不符、输入无效或交付路径冲突。"""


# ---------------------------------------------------------------- 台账打开

def _open_ledger_ro(path, label):
    """只读打开一份迁移台账并校验模式版本。"""
    if not os.path.exists(path):
        raise ApprovalError(f"{label}不存在: {path}")
    uri = "file:" + quote(os.path.abspath(path)) + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
        conn.isolation_level = None
        conn.execute("PRAGMA query_only = ON")
        row = conn.execute(
            "SELECT value FROM meta WHERE key = 'ledger_version'").fetchone()
    except sqlite3.Error as exc:
        raise ApprovalError(f"{label}无法读取: {path}: {exc}") from exc
    if row is None or row[0] != LEDGER_SCHEMA_VERSION:
        conn.close()
        raise ApprovalError(f"{label}不是有效的迁移台账: {path}")
    return conn


def _records_cursor(conn, period):
    return conn.execute(
        "SELECT sender, record_no, is_unknown, valuation, weight FROM records"
        " WHERE period = ? ORDER BY sender, record_no", (period,))


def _record_locator(row):
    return f"记录 (sender={row[0]!r}, record_no={row[1]})"


# ------------------------------------------------------------ 登记一致性

def _diff_field(rows):
    """三份同键登记不同时，返回首个不一致的字段名与各份取值。"""
    fields = ((2, "估值未知标记"), (3, "估值"), (4, "权重"))
    for idx, name in fields:
        vals = [r[idx] for r in rows]
        if any(v != vals[0] for v in vals):
            return name, vals
    return None, None


def _check_registrations(conns):
    """三稿旧端、新端记录登记逐条一致；不符指出稿、端、记录，O(1) 内存。"""
    for period in (0, 1):
        side = _SIDE_LABELS[period]
        cursors = [_records_cursor(c, period) for c in conns]
        try:
            rows = [next(cur, None) for cur in cursors]
            while any(r is not None for r in rows):
                if any(r is None for r in rows):
                    # 至少一稿已耗尽或该键缺失：定位最小现存键并指出缺稿
                    present = [r[:2] for r in rows if r is not None]
                    min_key = min(present)
                    missing = [_LEDGER_LABELS[i] for i in range(3)
                               if rows[i] is None or rows[i][:2] != min_key]
                    found = [_LEDGER_LABELS[i] for i in range(3)
                             if rows[i] is not None
                             and rows[i][:2] == min_key]
                    raise ApprovalError(
                        f"{side}记录登记不符: 记录"
                        f" (sender={min_key[0]!r}, record_no={min_key[1]})"
                        f" 仅见于{'、'.join(found)}，"
                        f"{'、'.join(missing)}缺少该记录")
                if any(r[:2] != rows[0][:2] for r in rows):
                    min_key = min(r[:2] for r in rows)
                    missing = [_LEDGER_LABELS[i] for i in range(3)
                               if rows[i][:2] != min_key]
                    found = [_LEDGER_LABELS[i] for i in range(3)
                             if rows[i][:2] == min_key]
                    raise ApprovalError(
                        f"{side}记录登记不符: 记录"
                        f" (sender={min_key[0]!r}, record_no={min_key[1]})"
                        f" 仅见于{'、'.join(found)}，"
                        f"{'、'.join(missing)}缺少该记录")
                if any(tuple(r) != tuple(rows[0]) for r in rows):
                    field, vals = _diff_field(rows)
                    detail = "，".join(
                        f"{_LEDGER_LABELS[i]}={vals[i]!r}" for i in range(3))
                    raise ApprovalError(
                        f"{side}记录登记不符: {_LEDGER_LABELS[0]}、"
                        f"{_LEDGER_LABELS[1]}、{_LEDGER_LABELS[2]}对"
                        f"{_record_locator(rows[0])}的{field}登记不一致"
                        f"（{detail}）")
                rows = [next(cur, None) for cur in cursors]
        finally:
            for cur in cursors:
                cur.close()


# ------------------------------------------------------------ 旧端三稿归并

def _transfer_cursor(conn, sender, record_no):
    """该台账中某旧端记录的申报转列段，按旧端副本位数值序，O(1) 内存。

    每行 (旧起, 旧终, 新sender, 新record_no, 新起, 长度)，整数运算在 Python
    大整数上完成，不依赖 SQLite 的 int64 域。
    """
    cur = conn.execute(
        "SELECT old_start, new_sender, new_record_no, new_start, length"
        " FROM entries WHERE kind = 'transfer'"
        " AND old_sender = ? AND old_record_no = ?"
        " ORDER BY length(old_start), old_start", (sender, record_no))
    try:
        for o_start_txt, n_sender, n_rec, n_start_txt, length_txt in cur:
            o_start = int(o_start_txt)
            length = int(length_txt)
            # 整数运算在 Python 大整数上完成，不依赖 SQLite 的 int64/REAL 域
            yield (o_start, o_start + length, n_sender, n_rec,
                   int(n_start_txt), length)
    finally:
        cur.close()


def _direction(row, lo):
    """台账转列段在旧端副本位 lo 处的指向 (新sender, 新record_no, 新start)。"""
    _o_start, _o_end, n_sender, n_rec, n_start, _length = row
    return (n_sender, n_rec, n_start + (lo - row[0]))


def _merge_old_record(streams):
    """归并一份旧端记录上的三稿转列段流，产出初等段。

    生成 (lo, hi, (t0, t1, t2))：t_i 为该稿在段首的指向，None 表示该稿在
    此段未声明。三稿都未声明的位置不产出。扫描位置 pos 单调推进，仅保留
    三个流的队首行，O(1) 内存。
    """
    fronts = [next(stream, None) for stream in streams]
    if all(f is None for f in fronts):
        return
    pos = min(f[0] for f in fronts if f is not None)
    while any(f is not None for f in fronts):
        # 丢弃在 pos 之前（含 pos）已耗尽的队首行，取流内下一段
        for i, f in enumerate(fronts):
            if f is not None and f[1] <= pos:
                fronts[i] = next(streams[i], None)
        starts = [f[0] for f in fronts if f is not None]
        if not starts:
            break
        nxt = min(starts)
        if nxt > pos:
            pos = nxt                # 三稿都未声明的空洞：跳过，不产出
            continue
        # 下一切点 = 覆盖 pos 的段的最小终点与未覆盖段的最小起点二者取小
        hi = min(f[1] if f[0] <= pos else f[0]
                 for f in fronts if f is not None)
        targets = tuple(_direction(f, pos)
                        if f is not None and f[0] <= pos else None
                        for f in fronts)
        yield pos, hi, targets
        pos = hi


def _classify(targets):
    """三稿指向 -> ("adopt", 指向|None) 或 ("dispute", 三稿指向)。"""
    t0, t1, t2 = targets
    if t1 == t2:
        return "adopt", t1            # 双稿一致（含都未声明/都撤回）
    if t1 == t0:
        return "adopt", t2            # 仅右稿变更（含撤回 None）
    if t2 == t0:
        return "adopt", t1            # 仅左稿变更（含撤回 None）
    return "dispute", targets         # 两稿都变且指向不同


# ------------------------------------------------------------ 旧端争议合段

class _OldDisputeWriter:
    """旧端争议段在线合段并逐行写暂存文件；只保留最近一段。"""

    def __init__(self, fh):
        self._fh = fh
        self._pending = None
        self.count = 0

    @staticmethod
    def _point(target):
        if target is None:
            return None
        sender, record_no, start = target
        return {"sender": sender, "record_no": record_no,
                "start": str(start)}

    @staticmethod
    def _compatible(pending_target, target, shift):
        if pending_target is None or target is None:
            return pending_target is None and target is None
        (p_sender, p_rec, p_start) = pending_target
        sender, rec, start = target
        return (p_sender == sender and p_rec == rec
                and p_start + shift == start)

    def feed(self, sender, record_no, lo, hi, targets):
        p = self._pending
        if (p is not None and p["sender"] == sender
                and p["record_no"] == record_no and p["end"] == lo):
            shift = lo - p["start"]
            if all(self._compatible(p["options"][i], targets[i], shift)
                   for i in range(3)):
                p["end"] = hi
                return
        self.flush()
        self._pending = {"sender": sender, "record_no": record_no,
                         "start": lo, "end": hi, "options": targets}

    def flush(self):
        p = self._pending
        if p is None:
            return
        self._pending = None
        line = {
            "type": "old",
            "old": {"sender": p["sender"], "record_no": p["record_no"],
                    "start": str(p["start"]),
                    "length": str(p["end"] - p["start"])},
            "options": {"base": self._point(p["options"][0]),
                        "left": self._point(p["options"][1]),
                        "right": self._point(p["options"][2])}}
        self._fh.write(json.dumps(line, ensure_ascii=False) + "\n")
        self.count += 1


# ------------------------------------------------------------ 第一遍：旧端

def _insert_resolved(work, sender, record_no, lo, hi, target):
    n_sender, n_rec, n_start = target
    n_end = n_start + (hi - lo)
    work.execute(
        "INSERT INTO resolved(o_sender, o_record_no, o_start_len, o_start_txt,"
        " o_end_len, o_end_txt, n_sender, n_record_no, n_start_len,"
        " n_start_txt, n_end_len, n_end_txt, length_txt)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (sender, record_no, len(str(lo)), str(lo), len(str(hi)), str(hi),
         n_sender, n_rec, len(str(n_start)), str(n_start),
         len(str(n_end)), str(n_end), str(hi - lo)))


def _scan_old_side(base_conn, conns, work, old_fh):
    """逐旧端记录归并三稿：判定段入工作库，旧端争议在线合段落暂存。"""
    writer = _OldDisputeWriter(old_fh)
    n_records = 0
    n_resolved = 0
    rec_cur = _records_cursor(base_conn, 0)
    try:
        for sender, record_no, _unk, _val, _weight in rec_cur:
            n_records += 1
            streams = [_transfer_cursor(c, sender, record_no) for c in conns]
            try:
                for lo, hi, targets in _merge_old_record(streams):
                    verdict, payload = _classify(targets)
                    if verdict == "dispute":
                        writer.feed(sender, record_no, lo, hi, targets)
                    elif payload is not None:       # 采用的指向（撤回/未声明不落库）
                        _insert_resolved(work, sender, record_no, lo, hi,
                                         payload)
                        n_resolved += 1
            finally:
                for stream in streams:
                    stream.close()
    finally:
        rec_cur.close()
    writer.flush()
    return n_records, n_resolved, writer.count


# ------------------------------------------------------------ 第二遍：新端

class _NewDisputeWriter:
    """新端争议段在线合段并逐行写暂存文件；只保留最近一段的占用者。"""

    def __init__(self, fh):
        self._fh = fh
        self._pending = None
        self.count = 0

    @staticmethod
    def _mergeable(pending, sender, record_no, lo, occ):
        if pending is None or pending["sender"] != sender \
                or pending["record_no"] != record_no \
                or pending["end"] != lo or len(pending["occ"]) != len(occ):
            return False
        for p, c in zip(pending["occ"], occ):
            po, co = p["old"], c["old"]
            if po["sender"] != co["sender"] or po["record_no"] != co["record_no"]:
                return False
            if int(po["start"]) + int(po["length"]) != int(co["start"]):
                return False
        return True

    def feed(self, sender, record_no, lo, hi, occ):
        p = self._pending
        if self._mergeable(p, sender, record_no, lo, occ):
            p["end"] = hi
            for item, c in zip(p["occ"], occ):
                item["old"]["length"] = str(
                    int(item["old"]["length"]) + int(c["old"]["length"]))
            return
        self.flush()
        self._pending = {"sender": sender, "record_no": record_no,
                         "start": lo, "end": hi, "occ": occ}

    def flush(self):
        p = self._pending
        if p is None:
            return
        self._pending = None
        line = {
            "type": "new",
            "new": {"sender": p["sender"], "record_no": p["record_no"],
                    "start": str(p["start"]),
                    "length": str(p["end"] - p["start"])},
            "occupants": p["occ"]}
        self._fh.write(json.dumps(line, ensure_ascii=False) + "\n")
        self.count += 1


def _active_occupants(active, lo, hi):
    """当前活跃占用者在新端 [lo, hi) 的旧端区间，按旧端标识与起点排序。

    active 为 (新终, 新起, 旧sender, 旧record_no, 旧起) 元组堆，仅含覆盖该
    区间的占用者；按旧端 (sender, record_no, 映射起点) 排序，不在内存攒该
    新端记录的全部占用。
    """
    occ = []
    for _n_end, n_start, o_sender, o_rec, o_start in active:
        mapped = o_start + (lo - n_start)
        occ.append({"old": {"sender": o_sender, "record_no": o_rec,
                            "start": str(mapped),
                            "length": str(hi - lo)}})
    occ.sort(key=lambda item: (item["old"]["sender"],
                               item["old"]["record_no"],
                               len(item["old"]["start"]),
                               item["old"]["start"]))
    return occ


def _sweep_new_record(work, sender, record_no, writer):
    """单条新端记录上的活跃集扫描：深度 >= 2 的区间即争议，流式产出。

    一次索引扫描取该记录的已判定指向，按新端起点有序；堆里只保留当前覆盖
    扫描位置的占用者，内存只与同时占用的最大深度成正比。
    """
    cur = work.execute(
        "SELECT n_start_txt, n_end_txt, o_sender, o_record_no, o_start_txt"
        " FROM resolved WHERE n_sender = ? AND n_record_no = ?"
        " ORDER BY n_start_len, n_start_txt", (sender, record_no))
    rows = ((int(ns), int(ne), os_, or_, int(ost))
            for ns, ne, os_, or_, ost in cur)
    pending_row = next(rows, None)
    active = []          # (新终, 新起, 旧sender, 旧record_no, 旧起)
    pos = pending_row[0] if pending_row is not None else None
    while pending_row is not None or active:
        # 1) 退场：半开区间在终点即不再覆盖
        while active and active[0][0] <= pos:
            heapq.heappop(active)
        # 2) 入场：同起点的指向一并入堆
        while pending_row is not None and pending_row[0] == pos:
            n_start, n_end, o_sender, o_rec, o_start = pending_row
            heapq.heappush(active,
                           (n_end, n_start, o_sender, o_rec, o_start))
            pending_row = next(rows, None)
        if not active:
            if pending_row is not None:
                pos = pending_row[0]
            continue
        # 3) 下一切点 = 最早终点与下一起点二者取小
        nxt = active[0][0]
        if pending_row is not None and pending_row[0] < nxt:
            nxt = pending_row[0]
        if len(active) >= 2 and nxt > pos:
            writer.feed(sender, record_no, pos, nxt,
                        _active_occupants(active, pos, nxt))
        pos = nxt
    cur.close()


def _scan_new_side(work, new_fh):
    """逐新端记录做活跃集扫描：同一副本位被 >=2 个旧端占用即新端争议。"""
    writer = _NewDisputeWriter(new_fh)
    keys = work.execute(
        "SELECT DISTINCT n_sender, n_record_no FROM resolved"
        " ORDER BY n_sender, n_record_no")
    for sender, record_no in keys:
        _sweep_new_record(work, sender, record_no, writer)
    writer.flush()
    return writer.count


# ------------------------------------------------------------ 第三遍：清单

def _write_manifest(work, fh):
    """按旧端 (sender, record_no, 数值起点) 流式输出合并清单，相邻合段。"""
    cur = work.execute(
        "SELECT o_sender, o_record_no, o_start_txt, o_end_txt, n_sender,"
        " n_record_no, n_start_txt FROM resolved"
        " ORDER BY o_sender, o_record_no, o_start_len, o_start_txt")
    pending = None
    n_lines = 0
    for o_sender, o_rec, o_start_txt, o_end_txt, n_sender, n_rec, n_start_txt \
            in cur:
        o_start, o_end = int(o_start_txt), int(o_end_txt)
        n_start = int(n_start_txt)
        if (pending is not None and pending["o_sender"] == o_sender
                and pending["o_rec"] == o_rec
                and pending["o_end"] == o_start
                and pending["n_sender"] == n_sender
                and pending["n_rec"] == n_rec
                and pending["n_end"] == n_start):
            pending["o_end"] = o_end
            pending["n_end"] = n_start + (o_end - o_start)
            continue
        if pending is not None:
            _emit_manifest_line(fh, pending)
            n_lines += 1
        pending = {"o_sender": o_sender, "o_rec": o_rec,
                   "o_start": o_start, "o_end": o_end,
                   "n_sender": n_sender, "n_rec": n_rec,
                   "n_end": n_start + (o_end - o_start)}
    if pending is not None:
        _emit_manifest_line(fh, pending)
        n_lines += 1
    return n_lines


def _emit_manifest_line(fh, p):
    line = {
        "old": {"sender": p["o_sender"], "record_no": p["o_rec"],
                "start": str(p["o_start"])},
        "new": {"sender": p["n_sender"], "record_no": p["n_rec"],
                "start": str(p["n_end"] - (p["o_end"] - p["o_start"]))},
        "length": str(p["o_end"] - p["o_start"])}
    fh.write(json.dumps(line, ensure_ascii=False) + "\n")


# ------------------------------------------------------------ 入口

def merge_approvals(base_ledger, left_ledger, right_ledger,
                    manifest_out, disputes_out):
    """合并原批件与左、右两份修订稿台账，交付清单或争议明细。

    参数：
      base_ledger     原批件经 build_ledger 生成的台账
      left_ledger     左稿台账（已试算成功）
      right_ledger    右稿台账（已试算成功）
      manifest_out    可出账时的合并清单输出路径（build_ledger 清单格式）
      disputes_out    待协调时的争议明细输出路径（JSONL）

    两份输出路径相同、任一目标已存在或其目录缺失时直接报错，不更改现有
    文件；登记不符或台账无效同样不交付。返回摘要 dict，status 区分
    可出账（"可出账"）与待协调（"待协调"），输入/处理错误抛 ApprovalError，
    不伪装成争议。
    """
    base_ledger, left_ledger = str(base_ledger), str(left_ledger)
    right_ledger = str(right_ledger)
    manifest_out, disputes_out = str(manifest_out), str(disputes_out)
    if os.path.abspath(manifest_out) == os.path.abspath(disputes_out):
        raise ApprovalError("合并清单与争议明细的输出路径不能相同")
    for path in (manifest_out, disputes_out):
        if os.path.exists(path):
            raise ApprovalError(f"目标已存在: {path}")
        out_dir = os.path.dirname(os.path.abspath(path))
        if not os.path.isdir(out_dir):
            raise ApprovalError(f"目标目录不存在: {out_dir}")
    m_dir = os.path.dirname(os.path.abspath(manifest_out))
    d_dir = os.path.dirname(os.path.abspath(disputes_out))
    token = f"{os.getpid()}-{secrets.token_hex(4)}"
    m_base = os.path.basename(manifest_out)
    d_base = os.path.basename(disputes_out)
    # 工作库与清单暂存放在清单目标目录；争议暂存放在争议目标目录：
    # os.link 不跨文件系统，暂存必须与各自目标同目录。
    work_path = os.path.join(m_dir, f".{m_base}.work-{token}.tmp")
    manifest_tmp = os.path.join(m_dir, f".{m_base}.out-{token}.tmp")
    old_tmp = os.path.join(d_dir, f".{d_base}.old-{token}.tmp")
    new_tmp = os.path.join(d_dir, f".{d_base}.new-{token}.tmp")
    disputes_tmp = os.path.join(d_dir, f".{d_base}.out-{token}.tmp")
    conns = []
    try:
        paths = (base_ledger, left_ledger, right_ledger)
        for path, label in zip(paths, _LEDGER_LABELS):
            conns.append(_open_ledger_ro(path, label))
        _check_registrations(conns)
        work = sqlite3.connect(work_path)
        work.isolation_level = None
        work.execute("PRAGMA journal_mode = OFF")
        work.execute("PRAGMA synchronous = OFF")
        work.executescript(_WORK_SCHEMA)
        try:
            # 第一遍：旧端三稿归并——判定段入工作库，旧端争议落暂存
            with open(old_tmp, "w", encoding="utf-8", newline="") as old_fh:
                n_records, n_resolved, n_old_disputes = _scan_old_side(
                    conns[0], conns, work, old_fh)
            # 第二遍：新端相容性——只用已判定指向检查重叠占用
            with open(new_tmp, "w", encoding="utf-8", newline="") as new_fh:
                n_new_disputes = _scan_new_side(work, new_fh)
            if n_old_disputes == 0 and n_new_disputes == 0:
                # 第三遍：无任何争议，流式交付合并清单
                with open(manifest_tmp, "w", encoding="utf-8",
                          newline="") as fh:
                    n_lines = _write_manifest(work, fh)
                _atomic_link(manifest_tmp, manifest_out)
                return {"status": STATUS_APPROVED, "out": manifest_out,
                        "declarations": n_lines,
                        "resolved_segments": n_resolved,
                        "old_records": n_records,
                        "disputes": {"old": 0, "new": 0}}
            # 有争议：旧端争议在前、新端争议在后，只交付争议明细，
            # 不生成任何清单（连部分清单也不交付）
            with open(disputes_tmp, "wb") as fh:
                for part in (old_tmp, new_tmp):
                    with open(part, "rb") as src:
                        while True:
                            chunk = src.read(1 << 20)
                            if not chunk:
                                break
                            fh.write(chunk)
            _atomic_link(disputes_tmp, disputes_out)
            return {"status": STATUS_DISPUTED, "out": disputes_out,
                    "declarations": 0, "resolved_segments": n_resolved,
                    "old_records": n_records,
                    "disputes": {"old": n_old_disputes,
                                 "new": n_new_disputes}}
        finally:
            work.close()
    finally:
        for conn in conns:
            conn.close()
        for path in (work_path, manifest_tmp, old_tmp, new_tmp, disputes_tmp):
            try:
                os.unlink(path)
            except OSError:
                pass


def _atomic_link(tmp_path, out_path):
    """落盘后原子链接到目标；目标已存在（含并发出现）则不覆盖。"""
    fd = os.open(tmp_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        os.link(tmp_path, out_path)
    except FileExistsError:
        raise ApprovalError(f"目标已存在: {out_path}") from None
