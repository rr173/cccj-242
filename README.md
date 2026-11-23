# 离线数据交换平台：可重开的有序访问数据集

各区域已排好序的估价分布分片在此汇成一个**可重开的有序访问数据集**。
行不展开权重，下游可反复按**全局加权位置**或**估值**取记录，两种查询共享同一套
秩定义，可互相交叉核对。

## 数据模型

- 分片 = 来件标识(`sender`，即分片身份) + 独立编号的记录 + 有序行；
  行 = `(记录号, 估值, 正整数权重)`。
- 估值为固定比例的带符号 64 位整数；`null`/`None` 表示未知值，未知值统一并列且排在最后。
- 全局次序：估值升序(未知居末) → 分片标识(字节序) → 源行号(分片内次序) → 副本位 `0..权重-1`。
  分片内同值行不要求先按记录号排序；不同分片的同值行组成一个连续全局并列组。
- 同值总权重与全局位置可超过 SQLite 有符号整数上限：所有权重与前缀和以十进制文本
  落盘，在 Python 大整数上运算；估值本身不含权重，限定在 int64 域。

## 关键设计

- **磁盘只存输入行与持久索引，不按权重扩张**：`rows` 每行存分片内局部前缀和
  `wsum_before`，配 `(sender, is_unknown, valuation, row_idx)` 索引与
  `UNIQUE(sender, record_no)`。
- **全局秩不落地**：查询时把至多 64 个活跃分片的局部前缀和相加合成。
  因此单分片更正只 `DELETE`+`INSERT` 该分片自己的行——不重读未更正分片的记录，
  也不逐条重写后续全局秩；后到分片的更正自然改变后续全部秩。
- **小页定位不重数**：位置→估值在估值域上二分(每步一次跨分片索引求和)，
  分片内用前缀和二分定位首行，不从第一组或第一源行重新计数。
- **流式构建/更正**：逐行验证(升序、未知居末、正整数权重、记录号唯一——
  唯一性由磁盘索引强制)，内存 O(1)，允许巨大同值组。
- **声明语义**：同身份同内容的重复声明幂等只算一次；同身份异内容、逆序、
  非法权重、同分片记录号重复均拒绝构建；不同身份下相同字段值仍分别贡献。
- **更正语义**：`correct_shard` 原子替换同一分片(新旧版本不同时贡献)，
  新版本完整验证失败则回滚、仍用以前的库；只维护当前版本。
- **可重开**：数据集是单个 SQLite 文件，新进程打开即可查询，不访问输入分片。

## API

```python
from ordered_exchange import Dataset, ShardError, load_shard

ds = Dataset.create("data.db", [load_shard("left.json"), ...])  # 建库
ds = Dataset("data.db")                                         # 重开

ds.declare_shard(sender, rows)   # "inserted" / "duplicate"；冲突抛 ShardError
ds.correct_shard(sender, rows)   # 原子更正；验证失败抛 ShardError，旧库不变

ds.rank_interval(value)  # value=None 指未知组
# -> {"present", "start", "end", "weight", "insertion"}
#    缺席值：present=False，区间为空 [insertion, insertion)

ds.slice(position, length)  # 零基全局加权位置 + 请求长度
# -> [{"sender","record_no","source_row","value","weight",
#      "group":(g,g+w), "span":(rs,re), "replicas":(lo,hi)}, ...]
#    每条来源记录给并列组半开秩区间与被覆盖的副本位区间，不逐副本输出
```

## 夹具与测试

`fixtures/`：

- `two_sides_mid_big/`：left/right 两边都有同值(5 与 10)，中间组(值 10)总权重
  `2^63+15`，超过 64 位；全局总量 `2^63+23`。
- `duplicate_identity/`：`alpha`/`alpha_copy`(同身份同内容，幂等)、
  `alpha_conflict`(同身份异内容，拒绝)、`beta_same_rows`(不同身份同内容，分别贡献)。
- `unknown_tail/`：未知值末组，含缺席值在未知组前的插入位。
- `correction/`：`aaa_first` 排最前，其 v2 版本使后续记录秩整体平移 +6。

`tests/test_dataset.py` 共 24 个用例，要点：

- 独立小数据展开模型逐页(所有位置×所有长度)、逐值(含空并列组插入位)交叉核对；
  单记录内部切分(`slice(3,4)` → 副本位 `[3,7)`)。
- 更正前头分片后：近末页与并列区间同时平移，更正后全集再与展开模型逐页核对；
  并用 SQL 跟踪日志 + 未更正分片行指纹证明更正没有重读/重写其他分片。
- 更正失败(逆序新版本)后旧库可查且内容不变；重开后查询、幂等声明、更正均可用。

运行：

```bash
cd /workspace && python3 -m unittest discover -s tests -v
```
