"""离线数据交换平台：可重开的有序访问数据集。

每个分片由来件标识(sender)、独立编号的记录(记录号)与有序行组成；
行 = (记录号, 估值, 正整数权重)，估值为带符号 64 位整数或 None(未知值)。
未知值互相并列且排在最后。

全局顺序：估值升序(未知值居末) -> 分片标识(字节序) -> 分片内行号 -> 副本位(0..权重-1)。
同估值跨分片构成一个连续并列组；同值总权重与全局位置可超过 SQLite 有符号
整数上限，因此所有权重/前缀和都以十进制文本存储，在 Python 大整数上运算。

持久层只存输入行与索引(不按权重展开)。全局秩不落地：查询时把至多 64 个
活跃分片的局部前缀和相加得到，因此单分片更正只替换该分片的行，既不重读
也未更正分片的记录，也不逐条重写后续全局秩。
"""

from __future__ import annotations

import hashlib
import os
import sqlite3

__all__ = ["Dataset", "ShardError", "MIN_VALUE", "MAX_VALUE", "MAX_SHARDS"]

SCHEMA_VERSION = "1"
MAX_SHARDS = 64                      # 查询至多使用 64 个活跃分片的索引
MIN_VALUE = -(1 << 63)               # 估值域：带符号 64 位整数
MAX_VALUE = (1 << 63) - 1
MAX_RECORD_NO = (1 << 63) - 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shards (
  sender       TEXT PRIMARY KEY,     -- 来件标识 = 分片身份
  n_rows       INTEGER NOT NULL,
  total_weight TEXT NOT NULL,        -- 十进制大整数
  content_hash TEXT NOT NULL,        -- 行内容摘要，用于重复声明去重
  version      INTEGER NOT NULL      -- 该分片当前版本号
);
CREATE TABLE IF NOT EXISTS rows (
  sender      TEXT NOT NULL,
  row_idx     INTEGER NOT NULL,      -- 来源行号：分片内次序(0 起)
  record_no   INTEGER NOT NULL,      -- 独立记录号：分片内唯一
  is_unknown  INTEGER NOT NULL CHECK (is_unknown IN (0, 1)),
  valuation   INTEGER,               -- NULL 当且仅当未知值
  weight      TEXT NOT NULL,         -- 十进制正整数(可超 int64)
  wsum_before TEXT NOT NULL,         -- 十进制大整数：分片内本行之前的累计权重
  PRIMARY KEY (sender, row_idx),
  UNIQUE (sender, record_no),
  CHECK ((is_unknown = 1) = (valuation IS NULL))
);
-- 供查询的持久索引：按分片+估值定位，不展开权重
CREATE INDEX IF NOT EXISTS rows_val_idx ON rows(sender, is_unknown, valuation, row_idx);
"""


class ShardError(Exception):
    """分片验证失败或声明冲突。"""


def _is_int(x):
    return isinstance(x, int) and not isinstance(x, bool)


def _normalize_sender(sender):
    if not isinstance(sender, str) or not sender:
        raise ShardError(f"来件标识必须是非空字符串: {sender!r}")
    return sender


def _normalize_value(value):
    if value is None:
        return None
    if not _is_int(value):
        raise ShardError(f"估值必须是带符号64位整数或 None(未知): {value!r}")
    if value < MIN_VALUE or value > MAX_VALUE:
        raise ShardError(f"估值超出带符号64位整数域: {value}")
    return value


def _normalize_weight(weight):
    if isinstance(weight, str):
        if not weight.isdigit():
            raise ShardError(f"权重必须是正整数: {weight!r}")
        weight = int(weight)
    if not _is_int(weight) or weight < 1:
        raise ShardError(f"权重必须是正整数: {weight!r}")
    return weight


def _normalize_record_no(record_no):
    if not _is_int(record_no) or record_no < 0 or record_no > MAX_RECORD_NO:
        raise ShardError(f"记录号必须是 [0, 2^63) 内的整数: {record_no!r}")
    return record_no


def _normalize_row(row):
    if isinstance(row, dict):
        try:
            return row["record"], row["value"], row["weight"]
        except KeyError as exc:
            raise ShardError(f"行缺少字段 {exc}: {row!r}") from exc
    try:
        record_no, value, weight = row
    except (TypeError, ValueError) as exc:
        raise ShardError(f"行必须是 (记录号, 估值, 权重): {row!r}") from exc
    return record_no, value, weight


class Dataset:
    """可重开的有序访问数据集。

    打开方式：
        ds = Dataset.create(path, shards)   # 建库；shards 为 (sender, rows) 可迭代
        ds = Dataset(path)                  # 重开已有库，不访问输入分片
    """

    MAX_SHARDS = MAX_SHARDS
    MIN_VALUE = MIN_VALUE
    MAX_VALUE = MAX_VALUE

    def __init__(self, path, *, trace=None):
        self._path = str(path)
        if not os.path.exists(self._path):
            raise ShardError(f"数据集不存在: {self._path}")
        self._conn = sqlite3.connect(self._path)
        self._conn.isolation_level = None  # 显式事务
        self._trace = trace
        self._bounds_cache = None
        try:
            row = self._one("SELECT value FROM meta WHERE key = 'schema_version'")
        except sqlite3.Error as exc:
            self._conn.close()
            raise ShardError(f"不是有效的数据集库: {self._path}") from exc
        if row is None or row[0] != SCHEMA_VERSION:
            self._conn.close()
            raise ShardError(f"数据集模式版本不兼容: {self._path}")

    # ------------------------------------------------------------------ 建库

    @classmethod
    def create(cls, path, shards=(), *, overwrite=False, trace=None):
        """新建数据集并依次声明各分片。任一声明失败即抛 ShardError。"""
        path = str(path)
        if os.path.exists(path):
            if not overwrite:
                raise ShardError(f"目标已存在: {path}")
            os.remove(path)
        conn = sqlite3.connect(path)
        conn.isolation_level = None
        conn.executescript(_SCHEMA)
        conn.execute("INSERT INTO meta(key, value) VALUES ('schema_version', ?)",
                     (SCHEMA_VERSION,))
        conn.execute("INSERT INTO meta(key, value) VALUES ('dataset_version', '0')")
        conn.close()
        ds = cls(path, trace=trace)
        for sender, rows in shards:
            ds.declare_shard(sender, rows)
        return ds

    # -------------------------------------------------------------- 底层执行

    def _exec(self, sql, params=()):
        if self._trace is not None:
            self._trace(sql, params)
        return self._conn.execute(sql, params)

    def _one(self, sql, params=()):
        return self._exec(sql, params).fetchone()

    def _all(self, sql, params=()):
        return self._exec(sql, params).fetchall()

    # -------------------------------------------------------------- 写入路径

    def declare_shard(self, sender, rows):
        """声明一个分片。

        返回 "inserted" / "duplicate"。
        相同身份且内容相同的重复声明只算一次(幂等)；同身份异内容拒绝构建。
        """
        return self._ingest(sender, rows, correct=False)

    def correct_shard(self, sender, rows):
        """用同一分片的新版本更正单个分片。

        新版本须完整验证；同一分片新旧版本不得一起贡献(原子替换)；
        验证失败抛 ShardError，旧库保持不变。只维护当前版本。
        """
        return self._ingest(sender, rows, correct=True)

    def _ingest(self, sender, rows, *, correct):
        sender = _normalize_sender(sender)
        existing = self._one("SELECT content_hash FROM shards WHERE sender = ?",
                             (sender,))
        if existing is None:
            if correct:
                raise ShardError(f"更正目标分片不存在: {sender!r}")
            if self.shard_count() >= MAX_SHARDS:
                raise ShardError(f"活跃分片数超过 {MAX_SHARDS} 上限")
        self._exec("BEGIN IMMEDIATE")
        try:
            if existing is not None:
                # 先移除旧版本再流入新版本；失败回滚即恢复旧版本，
                # 新旧版本不会同时贡献。其他分片的行完全不被触碰。
                self._exec("DELETE FROM rows WHERE sender = ?", (sender,))
            digest, n_rows, total = self._load_rows(sender, rows)
            if existing is not None and not correct:
                if digest == existing[0]:
                    self._exec("ROLLBACK")  # 内容相同的重复声明：幂等，只算一次
                    return "duplicate"
                raise ShardError(           # 回滚由 except 完成，旧内容恢复
                    f"分片身份冲突: 来件标识 {sender!r} 已存在且内容不同")
            if existing is None:
                self._exec(
                    "INSERT INTO shards(sender, n_rows, total_weight, content_hash,"
                    " version) VALUES (?, ?, ?, ?, 1)",
                    (sender, n_rows, str(total), digest))
                status = "inserted"
            else:
                self._exec(
                    "UPDATE shards SET n_rows = ?, total_weight = ?,"
                    " content_hash = ?, version = version + 1 WHERE sender = ?",
                    (n_rows, str(total), digest, sender))
                status = "corrected"
            self._exec("UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1"
                       " AS TEXT) WHERE key = 'dataset_version'")
            self._exec("COMMIT")
            return status
        except BaseException:
            try:
                self._exec("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    def _load_rows(self, sender, rows):
        """流式验证并写入一个分片的行；不在内存中积累整组记录。

        返回 (内容摘要, 行数, 总权重)。记录号唯一性由
        UNIQUE(sender, record_no) 索引在磁盘侧强制，内存 O(1)。
        """
        digest = hashlib.sha256()
        total = 0
        n_rows = 0
        prev_key = None
        for raw in rows:
            record_no, value, weight = _normalize_row(raw)
            record_no = _normalize_record_no(record_no)
            value = _normalize_value(value)
            weight = _normalize_weight(weight)
            key = (1, 0) if value is None else (0, value)
            if prev_key is not None and key < prev_key:
                raise ShardError(
                    f"分片 {sender!r} 的行逆序: 估值须升序且未知值居末")
            prev_key = key
            digest.update(
                f"{record_no}|{'U' if value is None else value}|{weight}\n"
                .encode("utf-8"))
            try:
                self._exec(
                    "INSERT INTO rows(sender, row_idx, record_no, is_unknown,"
                    " valuation, weight, wsum_before)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (sender, n_rows, record_no,
                     1 if value is None else 0, value,
                     str(weight), str(total)))
            except sqlite3.IntegrityError as exc:
                if "record_no" in str(exc):
                    raise ShardError(
                        f"分片 {sender!r} 内记录号重复: {record_no}") from exc
                raise
            total += weight
            n_rows += 1
        return digest.hexdigest(), n_rows, total

    # -------------------------------------------------------------- 只读信息

    def shard_count(self):
        return self._one("SELECT COUNT(*) FROM shards")[0]

    def dataset_version(self):
        return int(self._one(
            "SELECT value FROM meta WHERE key = 'dataset_version'")[0])

    def shard_info(self):
        """各活跃分片概要(按分片标识序)。"""
        return [
            {"sender": s, "rows": n, "total_weight": int(t),
             "version": v, "content_hash": h}
            for s, n, t, v, h in self._all(
                "SELECT sender, n_rows, total_weight, version, content_hash"
                " FROM shards ORDER BY sender")
        ]

    def dump_rows(self, sender):
        """导出一个分片当前存储的行(供审计/核对)。"""
        return [
            {"source_row": r[0], "record_no": r[1],
             "value": None if r[2] else r[3],
             "weight": int(r[4]), "wsum_before": int(r[5])}
            for r in self._all(
                "SELECT row_idx, record_no, is_unknown, valuation, weight,"
                " wsum_before FROM rows WHERE sender = ? ORDER BY row_idx",
                (sender,))
        ]

    def total_weight(self):
        return sum(int(t) for (t,) in self._all(
            "SELECT total_weight FROM shards"))

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    # ---------------------------------------------------- 查询原语：局部前缀

    def _prefix_map(self, value, cmp_op):
        """每个活跃分片中估值满足 (valuation <cmp_op> value 的补) 的累计权重。

        返回 {sender: int}。cmp_op='>=' 时得到“估值 < value 的权重和”，
        cmp_op='>' 时得到“估值 <= value 的权重和”。
        每分片一次索引定位(O(log 行数))，不从头重数。
        """
        sql = (
            "SELECT s.sender, COALESCE("
            f"(SELECT r.wsum_before FROM rows r WHERE r.sender = s.sender"
            f" AND r.is_unknown = 0 AND r.valuation {cmp_op} ?"
            " ORDER BY r.valuation, r.row_idx LIMIT 1),"
            "(SELECT r.wsum_before FROM rows r WHERE r.sender = s.sender"
            " AND r.is_unknown = 1 ORDER BY r.row_idx LIMIT 1),"
            "s.total_weight) FROM shards s")
        return {sender: int(txt) for sender, txt in self._all(sql, (value,))}

    def _prefix_maps(self, value):
        """返回 (lo_map, le_map)：各分片估值 < / <= value 的权重和(大整数)。

        value 为 None 时指未知值并列组(排在所有已知值之后)。
        """
        if value is not None:
            return self._prefix_map(value, ">="), self._prefix_map(value, ">")
        sql = (
            "SELECT s.sender, COALESCE("
            "(SELECT r.wsum_before FROM rows r WHERE r.sender = s.sender"
            " AND r.is_unknown = 1 ORDER BY r.row_idx LIMIT 1),"
            " s.total_weight), s.total_weight FROM shards s")
        lo_map, le_map = {}, {}
        for sender, lo_txt, le_txt in self._all(sql):
            lo_map[sender] = int(lo_txt)
            le_map[sender] = int(le_txt)
        return lo_map, le_map

    def _group_info(self, value):
        """并列组的全局半开秩区间 [g, g+w) 及各分片局部前缀。"""
        lo_map, le_map = self._prefix_maps(value)
        g = sum(lo_map.values())
        w = sum(le_map.values()) - g
        return g, w, lo_map, le_map

    def _known_bounds(self):
        """已知估值的 (最小, 最大)，按数据集版本缓存；全未知时为 (None, None)。"""
        version = self.dataset_version()
        if self._bounds_cache is not None and self._bounds_cache[0] == version:
            return self._bounds_cache[1]
        lo = hi = None
        for _sender, mn, mx in self._all(
                "SELECT sender, MIN(valuation), MAX(valuation) FROM rows"
                " WHERE is_unknown = 0 GROUP BY sender"):
            if mn is not None:
                lo = mn if lo is None else min(lo, mn)
                hi = mx if hi is None else max(hi, mx)
        self._bounds_cache = (version, (lo, hi))
        return lo, hi

    def _value_at(self, pos):
        """全局加权位置 pos 落在哪个估值上；None 表示未知值组。

        在估值域上二分“累计权重 > pos 的最小估值”，每步用各分片索引
        求和，不从第一组或第一源行重新计数。
        """
        mn, mx = self._known_bounds()
        if mn is None:
            return None
        lo, hi = mn, mx + 1
        while lo < hi:
            mid = (lo + hi) // 2
            if sum(self._prefix_map(mid, ">").values()) > pos:
                hi = mid
            else:
                lo = mid + 1
        return None if lo > mx else lo

    # -------------------------------------------------------------- 对外查询

    def rank_interval(self, value):
        """按估值查询并列组。

        返回 dict：
          present   该估值是否存在
          start/end 并列组半开秩区间 [start, end)
          weight    并列组精确权重(大整数)
          insertion 缺席值的插入位(== start，此时区间为空)
        """
        value = _normalize_value(value)
        g, w, _lo, _le = self._group_info(value)
        return {"value": value, "present": w > 0, "start": g, "end": g + w,
                "weight": w, "insertion": g}

    def slice(self, position, length):
        """按零基全局加权位置和请求长度定位，交付被切到的来源记录。

        返回列表，每项：
          sender/record_no/source_row/value/weight  来源记录标识与内容
          group     所在并列组的半开秩区间 (start, end)
          span      该记录自身的全局半开区间
          replicas  请求覆盖的该记录副本位区间 [lo, hi)，不逐副本输出
        """
        if not _is_int(position) or position < 0:
            raise ValueError(f"位置必须是非负整数: {position!r}")
        if not _is_int(length) or length < 0:
            raise ValueError(f"长度必须是非负整数: {length!r}")
        total = self.total_weight()
        if length == 0 or position >= total:
            return []
        end = min(position + length, total)
        out = []
        cur = position
        senders = [r[0] for r in self._all("SELECT sender FROM shards"
                                           " ORDER BY sender")]
        while cur < end:
            value = self._value_at(cur)
            g, w, lo_map, le_map = self._group_info(value)
            gend = g + w
            base = g  # 组内全局基点：按分片标识序累加各分片同值权重
            for sender in senders:
                w_s = le_map[sender] - lo_map[sender]
                if w_s:
                    seg_lo, seg_hi = base, base + w_s
                    if seg_hi > cur and seg_lo < end:
                        a = max(cur, seg_lo) - seg_lo + lo_map[sender]
                        b = min(end, seg_hi) - seg_lo + lo_map[sender]
                        self._extract(sender, value, a, b, g, gend,
                                      seg_lo, lo_map[sender], out)
                    base = seg_hi
            cur = gend
        return out

    def _extract(self, sender, value, a, b, g, gend, seg_lo, lo_s, out):
        """切出某分片内同值行与 [a, b)(分片内前缀坐标) 相交的部分。"""
        if value is None:
            row = self._one("SELECT MIN(row_idx), MAX(row_idx) FROM rows"
                            " WHERE sender = ? AND is_unknown = 1", (sender,))
        else:
            row = self._one("SELECT MIN(row_idx), MAX(row_idx) FROM rows"
                            " WHERE sender = ? AND is_unknown = 0"
                            " AND valuation = ?", (sender, value))
        i0, i1 = row
        # 二分：最大的 i ∈ [i0, i1] 使 wsum_before(i) <= a，不从头重数
        lo_i, hi_i = i0, i1
        while lo_i < hi_i:
            mid = (lo_i + hi_i + 1) // 2
            ws = int(self._one("SELECT wsum_before FROM rows"
                               " WHERE sender = ? AND row_idx = ?",
                               (sender, mid))[0])
            if ws <= a:
                lo_i = mid
            else:
                hi_i = mid - 1
        rows = self._all(
            "SELECT row_idx, record_no, is_unknown, valuation, weight,"
            " wsum_before FROM rows WHERE sender = ? AND row_idx BETWEEN ? AND ?"
            " ORDER BY row_idx", (sender, lo_i, i1))
        for row_idx, record_no, is_unk, val, w_txt, ws_txt in rows:
            ws = int(ws_txt)
            if ws >= b:
                break
            w = int(w_txt)
            rec_start = seg_lo + (ws - lo_s)
            rec_end = rec_start + w
            out.append({
                "sender": sender,
                "record_no": record_no,
                "source_row": row_idx,
                "value": None if is_unk else val,
                "weight": w,
                "group": (g, gend),
                "span": (rec_start, rec_end),
                "replicas": (max(a, ws) - ws, min(b, ws + w) - ws),
            })
