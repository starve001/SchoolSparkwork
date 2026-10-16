# 第 4 章　RDD 转 DataFrame

> 对应 notebook：`notebooks/04_RDD转DataFrame.ipynb`
> 对应脚本：`scripts/rdd_to_dataframe.py`
> 数据：`data/article.txt`

---

## 一、为什么要从 RDD 转 DataFrame

| 对比 | RDD | DataFrame |
| --- | --- | --- |
| 数据形态 | 每行是一个 Python/Scala 对象 | 每行是带**列名和类型**的字段 |
| 元信息 | 无 Schema，Spark 不知道字段结构 | 有 Schema（字段名 + 数据类型） |
| 能否按列名统计 | 不能，只能靠下标 | 能，且可被 SQL 引用 |
| 优化 | 无 Catalyst 优化 | 走 Catalyst 优化器 + Tungsten |

> 一句话：**DataFrame 就是给 RDD 装上了表头（Schema）。**

---

## 二、创建 DataFrame 的多种方式

### 2.1 从普通 RDD 用 toDF（隐式 Schema）

```python
rdd = sc.textFile("../data/article.txt") \
    .flatMap(lambda line: line.split()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

wordCounts = rdd.toDF(["word", "count"])
wordCounts.printSchema()
```

输出：

```
root
 |-- word: string (nullable = true)
 |-- count: long (nullable = true)
```

> `toDF()` 需要 RDD 的元素是**元组/列表**，列名按位置对应。

### 2.2 用 createDataFrame（显式列名）

```python
wordCounts = spark.createDataFrame(rdd, ["word", "count"])
```

与 `toDF()` 效果等价。

### 2.3 本地 list 直接建 DataFrame

```python
data = [("Alice", 21), ("Bob", 24)]
people = spark.createDataFrame(data, ["name", "age"])
people.show()
```

### 2.4 Pandas DataFrame 转 Spark DataFrame（Python API 特有）

```python
import pandas as pd

pandasDF = pd.DataFrame({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "city": ["Beijing", "Shanghai", "Shanghai"]
})
sparkDF = spark.createDataFrame(pandasDF)
sparkDF.printSchema()
sparkDF.show()
```

> 这是 **PySpark 特有的便利功能**，Java/Scala 没有；反方向用 `df.toPandas()`。

### 2.5 RDD → Row → StructType（生产推荐）

```python
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

row_rdd = rdd.map(lambda x: Row(word=x[0], count=x[1]))

schema = StructType([
    StructField("word", StringType(), True),
    StructField("count", IntegerType(), True),
])

wordCounts = spark.createDataFrame(row_rdd, schema)
```

**为什么生产环境推荐这种方式**：Schema 被**显式声明**，类型完全可控，不依赖推断，避免类型猜错。

---

## 三、四种创建方式对比

| 方式 | 写法 | 适用场景 |
| --- | --- | --- |
| `toDF(列名)` | `rdd.toDF(["word","count"])` | 快速转换，RDD 元素是元组 |
| `createDataFrame(rdd, 列名)` | `spark.createDataFrame(rdd, [...])` | 同上，等价写法 |
| `createDataFrame(list, 列名)` | 本地数据 | 小规模测试数据 |
| `createDataFrame(pandasDF)` | Pandas 对象 | Python 特有，本地 DataFrame 上云 |
| `createDataFrame(row_rdd, schema)` | 显式 StructType | **生产推荐**，类型可控 |

---

## 四、词频统计完整链路

```
textFile 读文本
   ↓ flatMap 按空格切词
单词 RDD
   ↓ map 打标 (word, 1)
(word, 1) 键值对 RDD
   ↓ reduceByKey 按词聚合
(word, count) RDD
   ↓ toDF / createDataFrame 加 Schema
DataFrame
   ↓ printSchema / show
带列名的二维表
```

```python
rdd = sc.textFile("../data/article.txt") \
    .flatMap(lambda line: line.split()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

wordCounts = rdd.toDF(["word", "count"])
wordCounts.printSchema()
wordCounts.show(5)
```

**要点**

- `flatMap` 用于「一行 → 多个词」，这是词频统计的关键；
- `reduceByKey` 输出 `(word, count)` 二元组，正好对应 DataFrame 的两列；
- `toDF` 的列名顺序必须与元组元素顺序一致。

---

## 五、本章小结与考点

1. **DataFrame = RDD + Schema**，因此能按列名统计、能被 SQL 引用；
2. RDD 转 DataFrame 常见四种方式：`toDF`、`createDataFrame(rdd, 列名)`、`createDataFrame(list, 列名)`、`createDataFrame(pandasDF)`；
3. 生产推荐 **Row + 显式 StructType**，类型可控；
4. Pandas → Spark DataFrame 是 **Python API 特有**功能；
5. 词频统计链路：`textFile → flatMap → map → reduceByKey → toDF`；
6. `printSchema()` 看结构，`show(n)` 看数据（默认 20 行）。
