"""离线数据交换平台：可重开的有序访问数据集。"""

from .dataset import Dataset, ShardError, MAX_SHARDS, MAX_VALUE, MIN_VALUE
from .shardfile import load_shard

__all__ = ["Dataset", "ShardError", "MAX_SHARDS", "MAX_VALUE", "MIN_VALUE",
           "load_shard"]
