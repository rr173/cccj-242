"""驻留资格判定：沿多期迁移台账回溯末期记录子区间的同地区连续驻留。

输入（核算期间输入不变）：
- 按闭账顺序给出的一份或多份相邻期迁移台账（build_ledger 的交付物）；
- 最低连续闭账期数（正整数）；
- 末期待核 JSONL 清单，每行 {"sender", "record_no", "start", "length"}
  指定一个末期记录内的半开副本位区间；start/length 为十进制整数字符串，
  起点非负、长度为正。各行独立，重复或相交的请求各自出结果；空行跳过
  但计入行号。

台账链：第一份台账的旧侧为第 0 期，第 i 份（1 起）连接第 i-1 期与第 i 期。
相邻两份在共用期的记录登记须逐条一致（记录标识、权重、估值；未知估值只与
未知相同——总量相等不能代替这项检查），不一致则不交付并指出共用期期号。

判定（以末期所在地区为准，按台账明细确定每一段的实际前身，两端连通时依照
明细里的相对副本位对应）：
- 默认承接与同地区转列都延续驻留：换号、偏移变化、估值变化不重新计时；
- 最近一次来自另一地区的转列、或没有旧端的新增，是该段驻留的起点，自其
  发生后的闭账期计满第一期；此前即使曾在本地区也不累计，退出后同号再
  新增不承接旧库存；
- 同地区前身一直能追到第 0 期的，只能确认从第 0 期起的下限，不能断言
  当期刚进入；
- 确切期数或已知下限达到门槛判「符合」，确切期数不足判「不符合」，只有
  下限且不足判「历史不足」。

交付一个新的 JSONL 文件，每行一个末期子区间：

    {"line_no", "sender", "record_no", "start", "length",
     "verdict", "periods", "exact",
     "basis": {"period", "sender", "record_no", "start", "entry_id"?}}

start/length/basis.start 为十进制字符串；exact=false 时 periods 是下限。
basis 定位驻留起点（或窗口最早闭账期）的期号、记录与对应子区间起点；由
转列或新增确定起点的附该台账（第 period 份）的 entry_id。按请求行号、
区间起点有序，每条请求不重不漏；相邻两段结论、依据期号、记录、明细引用
相同且依据端副本位连续时合为一段，否则分开。

记录找不到、坏行、区间越界或共用期不一致时不交付结果，错误指出请求行或
不一致的共用期；已有目标不覆盖，目标路径不出现半成品。只读使用既有台账，
不打开原始数据集或转列清单，也不重新核算单份台账。全程按区间流式处理：
副本位与权重可超有符号 64 位，不逐副本展开，不在内存保留整期记录或明细、
全部请求或某条热点记录的全部明细与追踪片段。
"""

from __future__ import annotations

import json
import os
import re
import secrets
import sqlite3
from urllib.parse import quote

from .dataset import MAX_RECORD_NO
from .ledger import LEDGER_SCHEMA_VERSION

__all__ = ["residency_report", "ResidencyError", "VERDICT_ELIGIBLE",
           "VERDICT_INELIGIBLE", "VERDICT_INSUFFICIENT"]

VERDICT_ELIGIBLE = "符合"        # 确切期数或已知下限达到门槛
VERDICT_INELIGIBLE = "不符合"    # 确切期数不足
VERDICT_INSUFFICIENT = "历史不足"  # 只有下限且不足

_DECIMAL_RE = re.compile(r"[0-9]+")


class ResidencyError(Exception):
    """请求验证失败、台账链在共用期不一致或交付路径冲突。"""


def _is_int(x):
    return isinstance(x, int) and not isinstance(x, bool)


# ---------------------------------------------------------------- 台账打开

def _open_ledger_ro(path, index):
    """只读打开一份迁移台账并校验模式版本；不重新核算其内容。"""
    label = f"第 {index} 份台账"
    if not os.path.exists(path):
        raise ResidencyError(f"{label}不存在: {path}")
    uri = "file:" + quote(os.path.abspath(path)) + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
        conn.isolation_level = None
        conn.execute("PRAGMA query_only = ON")
        row = conn.execute(
            "SELECT value FROM meta WHERE key = 'ledger_version'").fetchone()
    except sqlite3.Error as exc:
        raise ResidencyError(f"{label}无法读取: {path}: {exc}") from exc
    if row is None or row[0] != LEDGER_SCHEMA_VERSION:
        conn.close()
        raise ResidencyError(f"{label}不是有效的迁移台账: {path}")
    return conn


def _records_cursor(conn, period):
    return conn.execute(
        "SELECT sender, record_no, is_unknown, valuation, weight FROM records"
        " WHERE period = ? ORDER BY sender, record_no", (period,))


def _rec_desc(row):
    sender, record_no, is_unknown, valuation, weight = row
    value = "未知" if is_unknown else repr(valuation)
    return (f"(sender={sender!r}, record_no={record_no},"
            f" 权重={weight}, 估值={value})")


def _check_chain(conns):
    """相邻台账在共用期的记录登记逐条一致；不一致指出共用期期号。

    两份登记都按 (sender, record_no) 有序，归并比对，O(1) 内存。
    """
    for p in range(1, len(conns)):
        cur_a = _records_cursor(conns[p - 1], 1)  # 第 p 份的新侧 = 第 p 期
        cur_b = _records_cursor(conns[p], 0)      # 第 p+1 份的旧侧 = 第 p 期
        try:
            a = next(cur_a, None)
            b = next(cur_b, None)
            while a is not None and b is not None:
                if tuple(a) != tuple(b):
                    raise ResidencyError(
                        f"第 {p} 期（共用期）记录登记不一致: 第 {p} 份台账"
                        f"新侧 {_rec_desc(a)} 与第 {p + 1} 份台账旧侧"
                        f" {_rec_desc(b)} 不符")
                a = next(cur_a, None)
                b = next(cur_b, None)
            if a is not None or b is not None:
                row = a if a is not None else b
                where = (f"第 {p} 份台账新侧" if a is not None
                         else f"第 {p + 1} 份台账旧侧")
                raise ResidencyError(
                    f"第 {p} 期（共用期）记录登记不一致: 记录"
                    f" {_rec_desc(row)} 仅见于{where}")
        finally:
            cur_a.close()
            cur_b.close()


# ---------------------------------------------------------------- 请求解析

def _parse_request(line, line_no):
    """解析一条请求，返回 (sender, record_no, start, length)；坏行抛错。"""
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ResidencyError(
            f"请求第 {line_no} 行: 不是有效的 JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise ResidencyError(f"请求第 {line_no} 行: 请求必须是 JSON 对象")
    sender = obj.get("sender")
    if not isinstance(sender, str) or not sender:
        raise ResidencyError(
            f"请求第 {line_no} 行: sender 必须是非空字符串")
    record_no = obj.get("record_no")
    if not _is_int(record_no) or record_no < 0 or record_no > MAX_RECORD_NO:
        raise ResidencyError(
            f"请求第 {line_no} 行: record_no 必须是 [0, 2^63) 内的整数")
    start = obj.get("start")
    if not isinstance(start, str) or not _DECIMAL_RE.fullmatch(start):
        raise ResidencyError(
            f"请求第 {line_no} 行: start 必须是十进制整数字符串")
    length = obj.get("length")
    if not isinstance(length, str) or not _DECIMAL_RE.fullmatch(length):
        raise ResidencyError(
            f"请求第 {line_no} 行: length 必须是十进制整数字符串")
    length = int(length)
    if length < 1:
        raise ResidencyError(f"请求第 {line_no} 行: length 必须为正")
    return sender, record_no, int(start), length


def _final_record_weight(conn, sender, record_no, line_no):
    """末期（最后一份台账新侧）记录权重；记录不存在抛错并指出请求行。"""
    row = conn.execute(
        "SELECT weight FROM records WHERE period = 1 AND sender = ?"
        " AND record_no = ?", (sender, record_no)).fetchone()
    if row is None:
        raise ResidencyError(
            f"请求第 {line_no} 行: 末期记录不存在: sender={sender!r}"
            f" record_no={record_no}")
    return int(row[0])


# ---------------------------------------------------------------- 回溯引擎

class _Frame:
    """某一期上待回溯的一段区间；cur 流式给出覆盖该区间的台账明细。"""

    __slots__ = ("level", "sender", "record_no", "pos", "end", "final_pos",
                 "cur")

    def __init__(self, level, sender, record_no, pos, end, final_pos, cur):
        self.level = level          # 当前期号（1..N），用第 level 份台账回溯
        self.sender = sender        # 当前记录（始终处于末期所在地区）
        self.record_no = record_no
        self.pos = pos              # 当前期内剩余区间的起点
        self.end = end              # 当前期内区间终点（半开）
        self.final_pos = final_pos  # pos 对应的末期副本位
        self.cur = cur


class _Tracer:
    """沿台账链把末期一个区间逐段回溯；内存只与台账份数成正比。"""

    def __init__(self, conns):
        self._conns = conns
        self.periods = len(conns)   # 末期期号 N

    def _entry_cursor(self, level, sender, record_no, start):
        """覆盖 [start, ...) 的明细流（按副本位数值序），O(1) 内存。

        明细在记录上恰好铺满 [0, 权重)，含 start 的明细即 start 最大的
        那条；副本位按 (位数, 文本) 排序即数值序，由 entries_new_idx 支持。
        """
        conn = self._conns[level - 1]
        s_txt = str(start)
        row = conn.execute(
            "SELECT new_start FROM entries"
            " WHERE new_sender = ? AND new_record_no = ?"
            " AND (length(new_start), new_start) <= (?, ?)"
            " ORDER BY length(new_start) DESC, new_start DESC LIMIT 1",
            (sender, record_no, len(s_txt), s_txt)).fetchone()
        if row is None:
            raise ResidencyError(
                f"第 {level} 份台账缺少记录 (sender={sender!r},"
                f" record_no={record_no}) 的明细")
        base = row[0]
        return conn.execute(
            "SELECT entry_id, kind, old_sender, old_record_no, old_start,"
            " new_start, length FROM entries"
            " WHERE new_sender = ? AND new_record_no = ?"
            " AND (length(new_start), new_start) >= (?, ?)"
            " ORDER BY length(new_start), new_start",
            (sender, record_no, len(base), base))

    def fragments(self, sender, record_no, start, length):
        """按末期起点升序生成终端片段。

        片段 = (final_start, length, exact, periods, ev_period, ev_sender,
                ev_record_no, ev_start, entry_id)：
        由转列或新增确定起点的 exact=True、periods 为确切期数、entry_id 指向
        第 ev_period 份台账的明细；追溯到第 0 期的 exact=False、periods 为
        下限、entry_id 为 None。
        """
        n = self.periods
        stack = [_Frame(n, sender, record_no, start, start + length, start,
                        self._entry_cursor(n, sender, record_no, start))]
        try:
            while stack:
                fr = stack[-1]
                row = fr.cur.fetchone()
                if row is None or int(row[5]) >= fr.end:
                    fr.cur.close()
                    stack.pop()
                    if fr.pos != fr.end:
                        raise ResidencyError(
                            f"第 {fr.level} 份台账记录 (sender={fr.sender!r},"
                            f" record_no={fr.record_no}) 的明细在副本位"
                            f" {fr.pos} 处断裂")
                    continue
                (entry_id, kind, o_sender, o_rec, o_start_txt,
                 n_start_txt, length_txt) = row
                ns = int(n_start_txt)
                ne = ns + int(length_txt)
                lo = fr.pos if fr.pos > ns else ns
                hi = fr.end if fr.end < ne else ne
                if lo != fr.pos or lo >= hi:
                    raise ResidencyError(
                        f"第 {fr.level} 份台账记录 (sender={fr.sender!r},"
                        f" record_no={fr.record_no}) 的明细在副本位"
                        f" {fr.pos} 处断裂")
                frag_len = hi - lo
                final_lo = fr.final_pos
                fr.pos = hi
                fr.final_pos = final_lo + frag_len
                if kind == "new":
                    # 没有旧端的新增：驻留自第 fr.level 期起算
                    yield (final_lo, frag_len, True, n - fr.level + 1,
                           fr.level, fr.sender, fr.record_no, lo, entry_id)
                    continue
                if kind == "carry":
                    o_s, o_r, o_lo = fr.sender, fr.record_no, lo
                elif kind == "transfer":
                    if o_sender != fr.sender:
                        # 来自另一地区的转列：驻留自第 fr.level 期起算
                        yield (final_lo, frag_len, True, n - fr.level + 1,
                               fr.level, fr.sender, fr.record_no, lo,
                               entry_id)
                        continue
                    # 同地区转列：换号/偏移/估值变化不重新计时
                    o_s, o_r = o_sender, o_rec
                    o_lo = int(o_start_txt) + (lo - ns)
                else:
                    raise ResidencyError(
                        f"第 {fr.level} 份台账出现意外的明细类别: {kind!r}")
                if fr.level > 1:
                    stack.append(_Frame(
                        fr.level - 1, o_s, o_r, o_lo, o_lo + frag_len,
                        final_lo,
                        self._entry_cursor(fr.level - 1, o_s, o_r, o_lo)))
                else:
                    # 同地区前身追到第 0 期：只能确认下限
                    yield (final_lo, frag_len, False, n + 1,
                           0, o_s, o_r, o_lo, None)
        finally:
            for fr in stack:
                fr.cur.close()


# ---------------------------------------------------------------- 合并输出

class _SegmentWriter:
    """把同一请求的终端片段按规则合并并逐行写出；只保留最近一段。"""

    def __init__(self, fh, line_no, sender, record_no, min_periods):
        self._fh = fh
        self._line_no = line_no
        self._sender = sender
        self._record_no = record_no
        self._min = min_periods
        self._pending = None
        self.count = 0
        self.verdicts = {VERDICT_ELIGIBLE: 0, VERDICT_INELIGIBLE: 0,
                         VERDICT_INSUFFICIENT: 0}

    def feed(self, frag):
        (final_lo, frag_len, exact, periods, ev_period, ev_sender,
         ev_rec, ev_start, entry_id) = frag
        p = self._pending
        if (p is not None
                and p["exact"] == exact
                and p["ev_period"] == ev_period
                and p["ev_sender"] == ev_sender
                and p["ev_record_no"] == ev_rec
                and p["entry_id"] == entry_id
                and p["ev_start"] + p["length"] == ev_start
                and p["final_start"] + p["length"] == final_lo):
            # 结论、依据期号、记录、明细引用相同且依据端副本位连续：合段
            p["length"] += frag_len
            return
        self.flush()
        self._pending = {"final_start": final_lo, "length": frag_len,
                         "exact": exact, "periods": periods,
                         "ev_period": ev_period, "ev_sender": ev_sender,
                         "ev_record_no": ev_rec, "ev_start": ev_start,
                         "entry_id": entry_id}

    def flush(self):
        p = self._pending
        if p is None:
            return
        self._pending = None
        periods = p["periods"]
        if periods >= self._min:
            verdict = VERDICT_ELIGIBLE
        elif p["exact"]:
            verdict = VERDICT_INELIGIBLE
        else:
            verdict = VERDICT_INSUFFICIENT
        basis = {"period": p["ev_period"], "sender": p["ev_sender"],
                 "record_no": p["ev_record_no"], "start": str(p["ev_start"])}
        if p["entry_id"] is not None:
            basis["entry_id"] = p["entry_id"]
        line = {"line_no": self._line_no, "sender": self._sender,
                "record_no": self._record_no,
                "start": str(p["final_start"]), "length": str(p["length"]),
                "verdict": verdict, "periods": periods, "exact": p["exact"],
                "basis": basis}
        self._fh.write(json.dumps(line, ensure_ascii=False) + "\n")
        self.count += 1
        self.verdicts[verdict] += 1


# ---------------------------------------------------------------- 判定入口

def residency_report(ledger_paths, min_periods, requests_path, out_path):
    """驻留资格判定：交付末期各请求子区间的逐段结论与依据（JSONL）。

    参数：
      ledger_paths   按闭账顺序的一份或多份台账（第 1 份连接第 0、1 期）
      min_periods    最低连续闭账期数（正整数）
      requests_path  末期待核 JSONL 清单
      out_path       交付路径；已存在则拒绝覆盖

    任一请求行出错（坏行、记录找不到、区间越界）或共用期不一致时不交付，
    半成品不会出现在目标路径。返回摘要 dict：
    {"out", "requests", "segments", "verdicts", "ledgers"}。
    """
    if isinstance(ledger_paths, (str, bytes, os.PathLike)):
        ledger_paths = [ledger_paths]
    ledger_paths = [str(p) for p in ledger_paths]
    if not ledger_paths:
        raise ResidencyError("至少需要一份迁移台账")
    if not _is_int(min_periods) or min_periods < 1:
        raise ResidencyError(
            f"最低连续闭账期数必须是正整数: {min_periods!r}")
    requests_path = str(requests_path)
    out_path = str(out_path)
    if os.path.exists(out_path):
        raise ResidencyError(f"目标已存在: {out_path}")
    out_dir = os.path.dirname(os.path.abspath(out_path))
    if not os.path.isdir(out_dir):
        raise ResidencyError(f"目标目录不存在: {out_dir}")
    if not os.path.isfile(requests_path):
        raise ResidencyError(f"待核清单不存在: {requests_path}")
    conns = []
    tmp_path = None
    try:
        for i, p in enumerate(ledger_paths, 1):
            conns.append(_open_ledger_ro(p, i))
        _check_chain(conns)
        n = len(conns)
        final_conn = conns[-1]
        token = f"{os.getpid()}-{secrets.token_hex(4)}"
        base = os.path.basename(out_path)
        tmp_path = os.path.join(out_dir, f".{base}.residency-{token}.tmp")
        n_requests = 0
        n_segments = 0
        verdicts = {VERDICT_ELIGIBLE: 0, VERDICT_INELIGIBLE: 0,
                    VERDICT_INSUFFICIENT: 0}
        try:
            fh_in = open(requests_path, "r", encoding="utf-8", newline="")
        except OSError as exc:
            raise ResidencyError(
                f"待核清单无法读取: {requests_path}: {exc}") from exc
        tracer = _Tracer(conns)
        with fh_in:
            with open(tmp_path, "w", encoding="utf-8", newline="") as fh_out:
                try:
                    for line_no, line in enumerate(fh_in, 1):
                        if not line.strip():
                            continue
                        sender, record_no, start, length = _parse_request(
                            line, line_no)
                        weight = _final_record_weight(
                            final_conn, sender, record_no, line_no)
                        if start + length > weight:
                            raise ResidencyError(
                                f"请求第 {line_no} 行: 区间"
                                f" [{start}, {start + length}) 越过末期记录"
                                f"权重 {weight}")
                        writer = _SegmentWriter(fh_out, line_no, sender,
                                                record_no, min_periods)
                        gen = tracer.fragments(sender, record_no, start,
                                               length)
                        try:
                            for frag in gen:
                                writer.feed(frag)
                        finally:
                            gen.close()
                        writer.flush()
                        n_requests += 1
                        n_segments += writer.count
                        for k, v in writer.verdicts.items():
                            verdicts[k] += v
                except UnicodeDecodeError as exc:
                    raise ResidencyError(
                        f"待核清单不是有效的 UTF-8: {requests_path}") from exc
        # 落盘后原子链接到目标；目标已存在（含并发出现）则不覆盖
        fd = os.open(tmp_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        os.link(tmp_path, out_path)
        return {"out": out_path, "requests": n_requests,
                "segments": n_segments, "verdicts": verdicts,
                "ledgers": n}
    except FileExistsError:
        raise ResidencyError(f"目标已存在: {out_path}") from None
    finally:
        for conn in conns:
            conn.close()
        if tmp_path is not None:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
