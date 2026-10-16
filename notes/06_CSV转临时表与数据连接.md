# 第 6 章　CSV 转临时表与数据连接

> 对应 notebook：`notebooks/06_CSV转临时表与数据连接.ipynb`
> 数据：`data/Zipssortedbycitystate.csv`（城市邮编表，21379 行）、`data/sales_data.csv`

---

## 一、本章主线

```
CSV 文本
   ↓ sc.textFile + filter 表头 + split(",")
RDD[list]
   ↓ map 组装 Row
RDD[Row]
   ↓ createDataFrame(rows, schema)
DataFrame
   ↓ createOrReplaceTempView
临时表（可用 SQL 查询）
   ↓ join
连接结果
   ↓ toPandas
Pandas + matplotlib 绘图
```

---

## 二、CSV → 临时表

### 2.1 加载并切分

```python
RawZipRDD = sc.textFile("../data/Zipssortedbycitystate.csv")

zipRDD = RawZipRDD \
    .filter(lambda line: not line.startswith("CITY,STATE,POSTALCODE")) \
    .map(lambda line: line.split(","))

zipRows = zipRDD.map(lambda p: Row(
    CITY=p[0].strip(),
    STATE=p[1].strip(),
    POSTALCODE=p[2].strip()
))
```

> **过滤表头的两种写法**：`line != header`（与第一行比较）或 `not line.startswith("CITY")`（按前缀判断）。后者更快，但要求表头字段名固定。

### 2.2 显式 Schema

```python
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("CITY",       StringType(), nullable=False),
    StructField("STATE",      StringType(), nullable=False),
    StructField("POSTALCODE", StringType(), nullable=False),
])

df = spark.createDataFrame(zipRows, schema=schema)
df.createOrReplaceTempView("ziptable")
```

> **邮编必须用 StringType**：美国邮编存在前导 0（如 `04406`）。若用数值类型，前导 0 会丢失，变成 `4406`。**凡是编码类字段（邮编、电话、身份证、订单号）一律用字符串。**

### 2.3 查看与查询

```python
df.printSchema()
df.show(10)

# DataFrame API
df.filter("STATE = 'AL'").show()

# Spark SQL
spark.sql("select * from ziptable where STATE = 'AL'").show()
```

---

## 三、用 spark.read 读取 CSV

除了 `textFile` 手工切分，Spark 也提供内置 CSV 读取器：

```python
sale_df = (
    spark.read
    .option("header", "true")        # 第一行是表头
    .option("inferSchema", "true")   # 自动推断类型
    .option("quote", '"')            # 引号字符
    .option("escape", '"')           # 转义字符
    .csv("../data/sales_data.csv")
)

sale_df.createOrReplaceTempView("sale_table")
sale_df.printSchema()
```

| 方式 | 优点 | 缺点 |
| --- | --- | --- |
| `sc.textFile` + 手工切分 | 完全可控，适合脏数据 | 要自己处理表头、引号、类型 |
| `spark.read.csv` | 一行搞定，自动处理引号/转义 | 推断可能出错，需显式 schema 兜底 |

---

## 四、数据连接（Join）

### 4.1 连接动机

销售表里存在**邮编为空**的脏数据，需要用独立的城市邮编表把它补全。

```python
spark.sql("select count(*) from sale_table where POSTALCODE=''").show()
```

### 4.2 DataFrame 写法

```python
joined_df = sale_df.join(
    ZIP_df,
    sale_df.CITY == ZIP_df.CITY,     # 关联条件
    "left_outer"                     # 连接方式
)
joined_df.filter("ORDERNUMBER = 10159").distinct().show()
```

### 4.3 Spark SQL 写法

```python
spark.sql("""
    SELECT s.*, z.POSTALCODE AS ZIP_POSTALCODE
    FROM sale_table s
    LEFT JOIN zip_table z
      ON s.CITY = z.CITY
    WHERE s.ORDERNUMBER = 10159
""").show()
```

> **为什么要给右表邮编列起别名**：两张表都有 `POSTALCODE` 列，直接 `select *` 会**列名冲突**，结果里出现两个同名列。用 `AS ZIP_POSTALCODE` 区分。

### 4.4 连接方式（how）

| 方式 | 含义 |
| --- | --- |
| `inner` | 默认，只保留两边都匹配的行 |
| `left_outer` | 左表全保留，右表匹配不上补 NULL |
| `right_outer` | 右表全保留，左表匹配不上补 NULL |
| `full_outer` | 两边都全保留 |
| `left_semi` | 只保留左表中能匹配上的行（不含右表列） |
| `left_anti` | 只保留左表中**匹配不上**的行 |

---

## 五、toPandas 与绘图

```python
GroupByState_df = sale_df.groupBy('STATE').count().orderBy('STATE')
GroupByState_pandas_df = GroupByState_df.toPandas().set_index('STATE')
print(GroupByState_pandas_df)

import matplotlib.pyplot as plt
ax = GroupByState_pandas_df['count'].plot(
    kind='bar', title='Order Count by State',
    figsize=(14, 7), legend=True, fontsize=10)
ax.set_xlabel('STATE')
ax.set_ylabel('Order count')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()
```

> **先聚合再 toPandas**：`toPandas()` 会把整个分布式数据集**拉到 Driver 单机内存**。直接对原表调用可能 OOM，必须先 `groupBy` 聚合或 `limit`。

---

## 六、Pandas 索引基础（配套）

```python
import pandas as pd

people_data = [['Alex', 10], ['Bob', 12], ['Clarke', 13]]
people_df = pd.DataFrame(people_data, columns=['Name', 'Age'])

print(people_df.loc[1])       # 按标签取 -> Name: Bob, Age: 12
print(people_df.iloc[0])      # 按位置取 -> Name: Alex, Age: 10

people_df.set_index('Name', inplace=True)   # 把 Name 设为索引
print(people_df)
```

| 方法 | 依据 | 说明 |
| --- | --- | --- |
| `loc[]` | **标签**（label） | `loc[1]` 取索引为 1 的行 |
| `iloc[]` | **位置**（integer position） | `iloc[0]` 取第 0 行 |
| `set_index(col, inplace=True)` | 设置索引 | 原地修改，无返回值 |

> **`inplace=True` 的坑**：它原地修改、返回 `None`。如果写成 `df = df.set_index('Name', inplace=True)`，`df` 会变成 `None`。要么用 `inplace=True` 不接收返回值，要么用 `df = df.set_index('Name')`。

---

## 七、本章小结与考点

1. CSV → 临时表四步：`textFile` → 过滤表头 + `split` → `Row` → `createDataFrame`；
2. **编码类字段（邮编/电话/订单号）必须用 StringType**，否则前导 0 丢失；
3. `spark.read.csv` 内置读取器支持 `header` / `inferSchema` / `quote` / `escape`；
4. Join 三要素：**右表、关联条件、连接方式**；
5. 两表同名列要**用别名区分**，否则列名冲突；
6. 连接方式：`inner` / `left_outer` / `right_outer` / `full_outer` / `left_semi` / `left_anti`；
7. `toPandas()` 把数据拉到单机内存，**必须先聚合**；
8. Pandas 中 `loc` 按标签、`iloc` 按位置，`set_index(inplace=True)` 返回 `None`。
