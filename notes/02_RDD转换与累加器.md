# 第 2 章　RDD 转换与累加器

> 对应 notebook：`notebooks/02_RDD转换与累加器.ipynb`
> 对应脚本：`scripts/rdd_accumulator.py`

---

## 一、累加器（Accumulator）

### 1.1 为什么需要累加器

在分布式环境下，各 Executor 上的任务运行在不同进程/机器上，Driver 端定义的普通变量**不会**被 Executor 修改回来。累加器就是为「**只增不减的全局计数/求和**」设计的共享变量。

### 1.2 基本用法

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("rddDemo") \
    .master("local[1]") \
    .getOrCreate()
sc = spark.sparkContext

counter = sc.accumulator(0)          # 初始值 0
rdd = sc.parallelize(range(10))

def increment(x):
    counter.add(x)                   # 只能用 add()，不能用 +=

rdd.foreach(increment)
print("Counter value:", counter.value)   # 45 = 0+1+...+9
```

输出：

```
Counter value: 45
Rdd value: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

### 1.3 关键要点

| 要点 | 说明 |
| --- | --- |
| 创建位置 | 必须在 **Driver** 端用 `sc.accumulator(初始值)` 创建 |
| 修改方式 | Executor 端只能调用 `counter.add(x)`；**不能用 `counter += x`** |
| 读取方式 | 只能在 **Driver** 端读 `counter.value` |
| 语义 | 只能累加，不能读取当前值做判断（Executor 端读不到准确值） |

> **为什么不能用 `+=`**：`counter += x` 在 Python 里等价于 `counter = counter + x`，会让局部变量重新绑定，累加器对象本身没被修改；而且累加器**不支持 `__iadd__`**，会报错或静默失效。正确写法是 `counter.add(x)`。

> **在 notebook 里**：如果写成 `global counter` + `counter += x`，看起来能跑，但那是在**本地模式同一个进程**里的巧合，在集群上必然失败。

### 1.4 累加器的容错语义

任务重试或 stage 重算时，累加器可能被**重复累加**。因此累加器只适合「精确一次无所谓」或幂等场景的监控统计，不适合作为精确的业务计数。

---

## 二、广播变量（Broadcast Variable）

### 2.1 为什么需要广播变量

如果每个 Task 都需要一份只读的大表（如字典、维度表），默认做法是**每个 Task 各复制一份**，N 个 Task 就有 N 份，网络和内存开销巨大。

广播变量把这份数据**在每个 Executor 上只缓存一份**，该 Executor 上的所有 Task 共享读取。

### 2.2 基本用法

```python
b = sc.broadcast([1, 2, 3, 4, 5, 6])     # 在 Driver 端广播
print(b.value)                            # [1, 2, 3, 4, 5, 6]

b1 = sc.parallelize([0, 0]).flatMap(lambda x: b.value).collect()
print(b1)                                 # [1,2,3,4,5,6,1,2,3,4,5,6]

b.unpersist()                             # 不再需要时释放
```

### 2.3 关键要点

| 要点 | 说明 |
| --- | --- |
| 创建 | Driver 端 `sc.broadcast(数据)` |
| 读取 | Executor 端通过 `.value` 读取 |
| 权限 | **只读**，Executor 不能修改 |
| 存储 | 每个 Executor 一份，不是每个 Task 一份 |
| 释放 | `unpersist()` 主动清理 |

> **只读的原因**：广播变量是「分发到各 Executor 的副本」，如果允许修改，各节点副本会不一致，无法保证语义。

### 2.4 累加器 vs 广播变量

| 对比 | 累加器 Accumulator | 广播变量 Broadcast |
| --- | --- | --- |
| 方向 | Executor → Driver（汇总回来） | Driver → Executor（分发下去） |
| 读写 | Executor 可写（add），Driver 读 | Driver 写，Executor 只读 |
| 典型用途 | 计数、求和、监控指标 | 小表 join、字典查找、配置项 |

---

## 三、RDD 不存储数据

RDD 是**弹性分布式数据集**，但它本身**不存储数据**，只记录**计算逻辑（血缘 lineage）**。

```python
b1 = sc.parallelize([0, 0]).flatMap(lambda x: b.value).collect()
```

这条语句说明了这一点：`sc.parallelize([0, 0])` 只提供两个「占位」元素，真正的数据来自广播变量 `b.value`。RDD 保存的是「怎么算」，数据在 Action 触发时才实际流动。

> **含义**：RDD 被转换后不会立刻产生新数据；每一步都只是往血缘图上追加一个节点。

---

## 四、本章小结与考点

1. 累加器在 **Driver 创建**，Executor 端用 **`add()`** 修改（不能 `+=`），Driver 端用 `.value` 读取；
2. 累加器只适合累加型统计，任务重试可能重复累加；
3. 广播变量把只读数据**每个 Executor 缓存一份**，供所有 Task 共享，降低网络与内存开销；
4. 广播变量在 Executor 端**只读**，用完可 `unpersist()` 释放；
5. 累加器方向是「向上汇总」，广播变量方向是「向下分发」；
6. **RDD 不存储数据**，只记录计算逻辑与血缘关系。
