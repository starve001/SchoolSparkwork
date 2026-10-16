# 第 1 章　RDD 基础操作

> 对应 notebook：`notebooks/01_RDD基础操作.ipynb`
> 对应脚本：`scripts/rdd_basics.py`、`scripts/rdd_practice.py`
> 数据：`data/score.txt`

---

## 一、SparkSession 与 SparkContext

一切 Spark 程序的入口。

```python
from pyspark.sql import SparkSession

sparkBuilder = SparkSession.builder \
    .appName("rddDemo") \
    .master("local[1]") \
    .getOrCreate()
sc = sparkBuilder.sparkContext
```

| 配置 | 含义 |
| --- | --- |
| `appName("rddDemo")` | 应用名，显示在 Web UI（默认 `http://localhost:4040`） |
| `master("local[1]")` | 本地模式，用 1 个线程；`local[*]` 用全部核心 |
| `getOrCreate()` | 已有 session 则复用，否则新建 |

> **`SparkSession` 与 `SparkContext` 的关系**：`SparkSession` 是 Spark 2.0 之后的统一入口（封装了老的 `SQLContext` / `HiveContext`），`sparkContext` 从中取出底层的 `SparkContext`（`sc`）。
> **`sc` 管 RDD，`spark` 管 DataFrame / SQL。**

---

## 二、创建 RDD

### 2.1 从本地集合创建：parallelize

```python
rdd = sc.parallelize(range(1, 11))
print(rdd.collect())          # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
```

### 2.2 从外部文件创建：textFile

```python
rdd = sc.textFile("../data/score.txt")
```

**要点**

- `textFile` 读进来是**按行切分**的 RDD，每个元素是一行字符串；
- 路径可以是本地路径，也可以是 HDFS 路径。

---

## 三、常用转换算子（Transformation）

| 算子 | 作用 | 是否产生 shuffle |
| --- | --- | --- |
| `map(f)` | 每个元素经 `f` 映射为**一个**输出 | 否 |
| `mapPartitions(f)` | 以**分区**为单位处理，`f` 接收迭代器、返回迭代器 | 否 |
| `flatMap(f)` | 每个元素经 `f` 映射为 **0 到多个**输出（先 map 再拍平） | 否 |
| `filter(f)` | 保留使 `f` 返回 True 的元素 | 否 |
| `distinct()` | 去重 | 是 |
| `union(other)` | 并集（**不去重**） | 否 |
| `intersection(other)` | 交集（去重） | 是 |
| `reduceByKey(f)` | 按 key 聚合，`f` 是两两合并函数 | 是 |
| `sortBy(f)` | 按 `f` 排序，可指定 `ascending` | 是 |

### 3.1 map 与 mapPartitions 的区别

```python
# map：逐元素处理
rdd = sc.parallelize(range(1, 11))
rdd.map(lambda x: x * 2).collect()

# mapPartitions：逐分区处理，函数接收的是迭代器
rdd = sc.parallelize(range(10, 21))
rdd.mapPartitions(lambda it: [x * 2 for x in it]).collect()
```

> **性能差异**：`mapPartitions` 每个分区只调用一次函数，适合需要建立连接/初始化资源的场景（如写数据库），比 `map` 逐个元素调用开销小；但一次处理整个分区的数据，内存压力更大。

### 3.2 flatMap 与 map 的区别

- `map`：**一对一**，N 个输入 → N 个输出；
- `flatMap`：**一对多**，N 个输入 → 任意个输出（会自动拍平一层）。

```python
sc.parallelize(["hello world", "hello spark"]) \
  .flatMap(lambda line: line.split()) \
  .collect()
# ['hello', 'world', 'hello', 'spark']
```

### 3.3 并集与交集

```python
rdd1 = sc.parallelize([1, 2, 3, 4, 5])
rdd2 = sc.parallelize([4, 5, 6, 7, 8])

rdd1.union(rdd2).collect()          # [1,2,3,4,5,4,5,6,7,8] —— 不去重
rdd1.intersection(rdd2).collect()   # [4, 5]
```

> **易错点**：`union` 只是简单拼接，**不去重**；要去重需再接 `.distinct()`。

### 3.4 filter 与 distinct

```python
names = ['xiaoli', 'laoli', 'laowang', 'xiaoming', 'xiaojing', 'xiaokong']
sc.parallelize(names).filter(lambda x: "xiao" in x).collect()

data = [10, 10, 2, 5, 3, 5, 3, 6, 9, 1]
sc.parallelize(data).distinct().collect()
```

---

## 四、行动算子（Action）

只有 Action 才会真正触发计算（**惰性求值**）。

| 算子 | 作用 |
| --- | --- |
| `collect()` | 把所有元素**拉回 Driver**，返回列表 |
| `count()` | 元素个数 |
| `take(n)` | 取前 n 个 |
| `first()` | 取第 1 个 |
| `reduce(f)` | 两两聚合 |
| `foreach(f)` | 对每个元素执行 `f`（在 Executor 端执行，无返回值） |

> **`collect()` 的风险**：数据量大时会把全部结果拉到单机内存，可能 OOM。生产上优先用 `take(n)` 或聚合后再取。

---

## 五、综合案例：学生成绩统计

### 5.1 数据

`data/score.txt`：

```
张三,语文,88
张三,数学,92
李四,语文,78
李四,数学,85
王五,语文,90
王五,数学,76
```

### 5.2 转成 (姓名, 分数)

```python
rdd = sc.textFile("../data/score.txt")

rdd1 = rdd.map(lambda x: x.split(",")) \
          .map(lambda x: (x[0], float(x[2])))

print(rdd1.collect())
# [('张三', 88.0), ('张三', 92.0), ('李四', 78.0), ('李四', 85.0), ('王五', 90.0), ('王五', 76.0)]
```

> **为什么用 `map` 而不是 `flatMap`**：这里一行数据对应一条记录，是**一对一**关系，所以用 `map`；用 `flatMap` 会把元组拆散。

### 5.3 计算总分

```python
rdd2 = rdd1.reduceByKey(lambda x, y: x + y)
print(rdd2.collect())
# [('张三', 180.0), ('李四', 163.0), ('王五', 166.0)]
```

`reduceByKey` 会先在**每个分区内**局部聚合（combiner），再跨分区合并，比 `groupByKey` 高效。

### 5.4 按总分降序排序

```python
rdd3 = rdd2.sortBy(lambda x: x[1], ascending=False)
print(rdd3.collect())
# [('张三', 180.0), ('王五', 166.0), ('李四', 163.0)]
```

---

## 六、本章小结与考点

1. `SparkSession` 是统一入口，`sparkContext` 取出 `sc`；
2. 创建 RDD 两种方式：`sc.parallelize(集合)` / `sc.textFile(路径)`；
3. `map` 一对一、`flatMap` 一对多、`mapPartitions` 以分区为单位；
4. `union` **不去重**，`distinct` 去重，`intersection` 求交集；
5. **惰性求值**：Transformation 不触发计算，Action 才触发；
6. `reduceByKey` 有分区内预聚合，优于 `groupByKey`；
7. `collect()` 把数据全拉回 Driver，大数据集上慎用。
