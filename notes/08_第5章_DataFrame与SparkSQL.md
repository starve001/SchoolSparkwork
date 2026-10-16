# 第 5 章 DataFrame 与 Spark SQL —— 知识点总结

> 来源：瑞翼教育《Spark 大数据技术与应用》第五章三份课件
> `1-DataFrame.pdf` / `2-spark sql.pdf` / `3-dataframe与sparksql常用操作.pdf`
> 配套可运行代码：同目录 `第5章-DataFrame与SparkSQL.ipynb`（本机已实跑出结果）

---

## 0. 本章主线

```
RDD（行式对象，无列名）
   ↓ 加 Schema（列名 + 类型）
DataFrame（二维表，分布式）
   ↓ registerTempTable 注册成临时表
Spark SQL（可以用 SQL 语句查）
   ↓ toPandas()
Pandas + matplotlib（本地绘图）
```

一句话：**DataFrame 是给 RDD 装上表头，Spark SQL 是给 DataFrame 装上临时表名。**

---

## 1. DataFrame

### 1.1 是什么

在 Spark 中，DataFrame 以 RDD 为基础，是一种**分布式数据集**，与传统数据库中的**二维表格**相类似。

课件那张对比图的意思：

| RDD[Person] | DataFrame |
| --- | --- |
| 每行是一个 `Person` 对象，Spark 不知道里面有什么字段 | 每行是 `Name / Age / Height` 三列，类型分别是 `String / Int / Double` |
| 只能靠 Scala/Python 类型系统 | 带 Schema 元信息，能按列名做统计、能被 SQL 引用 |

### 1.2 三种创建方式

```python
# ① 把普通 RDD 转变为 DataFrame：reduceByKey 出来的 (word, count) 元组 RDD
rdd = sc.textFile("article.txt") \
         .flatMap(lambda line: line.split()) \
         .map(lambda word: (word, 1)) \
         .reduceByKey(lambda a, b: a + b)
wordCounts = sqlContext.createDataFrame(rdd, ["word", "count"])

# ② 把本地数据容器（list of tuple）转变为 DataFrame
data = [("Alice", 21), ("Bob", 24)]
people = sqlContext.createDataFrame(data, ["name", "age"])

# ③ 把 Pandas DataFrame 转变为 Spark DataFrame（Python API 特有功能）
sparkDF = sqlContext.createDataFrame(pandasDF)
```

第 ③ 种是 Python API 独有的便利功能，Java/Scala 没有；反方向用 `df.toPandas()`。

---

## 2. Spark SQL

### 2.1 来历：Shark 与 Hive

- **Shark 是 Spark SQL 的前身**，一种分布式 SQL 查询工具，设计目标就是**兼容 Hive**。
- 对比 Hive 架构图可以看出差异：Hive 的底层执行引擎是 **MapReduce**，Shark/Spark SQL 换成了 **Spark**，并且多了 **Cache Mgr.（缓存管理）**；SQL Parser、Query Optimizer、Physical Plan Execution 这套骨架是从 Hive 沿用过来的。

### 2.2 定义

Spark SQL 是 **Spark Core 之上的一个组件**，它引入了一个称为 **SchemaRDD** 的新数据抽象，为**结构化和半结构化数据**提供支持。

> SchemaRDD 就是 DataFrame 的前身名字（Spark 1.3 之后改名 DataFrame / Dataset），考试里出现 SchemaRDD 指的就是带 Schema 的 RDD。

### 2.3 五个特点

| 特点 | 含义 |
| --- | --- |
| 集成（Integration） | 与 Spark Core / MLlib / GraphX 无缝混用，SQL 结果还是 RDD |
| 兼容性（Compatibility） | 兼容已有 Hive 仓库、Hive SQL 语法 |
| 可扩展性（Extensibility） | 可自定义 UDF、自定义数据源、自己接优化规则 |
| 统一数据访问（Unified Data Access） | 一套接口读 Hive、MySQL、CSV、JSON、Parquet |
| 标准连接（Standard Connectivity） | 提供 JDBC/ODBC（Thrift Server）给外部工具连 |

### 2.4 执行原理

课件给的流程是两步：

1. **解析 → 绑定 → 计划 → 执行**（对应图上的 `Query → Parse → Bind → Optimize → Execute`）
2. 一条 SQL 被拆成三块：**Result（要什么）→ Data Source（从哪来）→ Operation（怎么算）**

```
Projection      Data Source     Filter
   ↓                ↓              ↓
SELECT a1,a2,a3  FROM tableA  WHERE condition
   ↓                ↓              ↓
 Result        Data Source      Operation
```

日常开发里还会遇到更细的四阶段说法（分析器 → 优化器 → 规划器 → 执行器），和课件这版是一个意思。

### 2.5 Spark SQL 的创建前提

- **Spark SQL 是由 DataFrame 派生出来的**：必须先创建 DataFrame，再通过登录 Spark SQL temp table，才能使用 Spark SQL 语句。
- 使用门槛低：即使非程序设计人员，只要懂 SQL 就能上手。
- **DataFrame 被创建时必须定义 Schema**，定义每一个字段名与数据类型，因而可以用字段名进行统计。

### 2.6 完整落地流程（课件 5 步）

数据源：`sales_data.csv`（销售信息表）和 `Zipssortedbycitystate.csv`（城市邮编表）。

重点字段：

| 字段 | 含义 | | 字段 | 含义 |
| --- | --- | --- | --- | --- |
| ORDERNUMBER | 定单号 | | PRODUCTCODE | 产品编号 |
| QUANTITYORDERED | 销量 | | CUSTOMERNAME | 消费者 |
| PRICEEACH | 单价 | | POSTALCODE | 邮编 |
| ORDERLINENUMBER | 订单行号 | | CITY | 城市 |
| ORDERDATE | 销售时间 | | STATE | 州 |
| YEAR_ID | 销售年份 | | PRODUCTLINE | 生产线 |

```python
# (1) 读取文本文件并查看数据数
RawSalesDataRDD = sc.textFile("../data/sales_data.csv")
RawSalesDataRDD.count()                      # 课件输出 2823

# (2) 查看前 5 项数据
RawSalesDataRDD.take(5)

# (3) 按照 "," 符号获取每一个字段
salesRDD = RawSalesDataRDD.map(lambda line: line.split(","))

# (4) 使用 RDD 创建 DataFrame
sqlContext = SparkSession.builder.getOrCreate()
sale_Rows = salesRDD.map(lambda p: Row(ORDERNUMBER=p[0], QUANTITYORDERED=p[1],
                                       PRICEEACH=p[2], ORDERLINENUMBER=p[3],
                                       SALES=p[4], ORDERDATE=p[5], ...,
                                       CONTACTFIRSTNAME=p[23]))
sale_df = sqlContext.createDataFrame(sale_Rows)
sale_df.printSchema()                        # 24 个字段全是 string

# (5) 使用 Spark SQL
sale_df.registerTempTable("sale_table")
sqlContext.sql("select count(*) counts from sale_table").show()   # 2823
```

**这里最容易考的点**：因为整行是 `split(",")` 切出来的，所有字段都是 **string 类型**。后面参与加减运算时，Spark 会隐式 `CAST(... AS DOUBLE)`，所以 `(2018 - YEAR_ID)` 的结果显示成 `15.0` 而不是 `15`。

---

## 3. Spark SQL 与 DataFrame 的常用操作（7 项）

课件每一项都给了**两种等价写法**，考试和实验报告经常要求互相改写：

| # | 操作 | DataFrame API | Spark SQL |
| --- | --- | --- | --- |
| 1 | 字段计算 | `sale_df.select("ORDERNUMBER","PRODUCTCODE",(2018-sale_df.YEAR_ID))` | `select ORDERNUMBER, PRODUCTCODE, (2018 - YEAR_ID) from sale_table` |
| 2 | 条件查询 | `sale_df.filter("YEAR_ID='2003'")` | `select * from sale_table where YEAR_ID='2003'` |
| 3 | 数据排序 | `.orderBy("YEAR_ID")` | `order by YEAR_ID` |
| 4 | 数据去重 | `.distinct()` | `select distinct PRODUCTCODE` |
| 5 | 分组统计 | `.groupby("PRODUCTCODE").count()` | `group by PRODUCTCODE` + `count(*)` |
| 6 | 数据连接 | `.join(ZIP_df, sale_df.CITY == ZIP_df.CITY, "left_outer")` | `left join zip_table z on s.CITY = z.CITY` |
| 7 | 数据绘图 | `df.toPandas()` + matplotlib | ——（SQL 只负责取数，画图仍走 Pandas） |

最后统一用 `.show(5)` 显示 5 条、`.show()` 默认 20 条。

### 3.1 字段计算

```python
sale_df.select("ORDERNUMBER", "PRODUCTCODE", (2018 - sale_df.YEAR_ID)).show(5)
```

| 程序代码 | 说明 |
| --- | --- |
| `sale_df` | 使用 sale_df 这个 DataFrame 进行统计 |
| `select("ORDERNUMBER", "PRODUCTCODE", (2018 - sale_df.YEAR_ID))` | 设置订单号、商品编码、销售年份差 |
| `show(5)` | 显示 5 条数据 |

计算列默认列名就是表达式本身（`(2018 - YEAR_ID)`），要改名用 `.alias("YEAR_DIFF")` 或 SQL 的 `AS`。

### 3.2 条件查询

`filter()` 里传的是**字符串表达式**，写法就是 SQL 的 `where` 后半段，别和 Python 的布尔表达式混。

### 3.3 数据排序

`orderBy()` 放在 `select()` 之后；降序用 `col("SALES").desc()`，SQL 里写 `order by ... desc`。

### 3.4 数据去重

`distinct()` 作用在**已经 select 出来的列**上，所以先 `select("PRODUCTCODE")` 再去重，得到的是产品编号清单；直接对整表 `distinct()` 是整行去重。

### 3.5 分组统计

```python
sale_df.select("PRODUCTCODE").groupby("PRODUCTCODE").count().show()
```

> 课件说明表里把 `count()` 写成「计算总和」，这是**笔误**：`count()` 是**计数（组内行数）**，求和要用 `sum()`、求平均用 `avg()`，且这两个要从 `pyspark.sql.functions` 里导入。

本机实测（5 行数据、产品编号全是 `S10_1678`）：

```
+-----------+-----+
|PRODUCTCODE|count|
+-----------+-----+
| "S10_1678"|    5|
+-----------+-----+
```

### 3.6 数据连接

先验证「销售表里有邮编为空的脏数据」，这是要用邮编表补全的动机：

```python
sqlContext.sql("select count(*) from sale_table where POSTALCODE=''").show()   # 课件 901 条
```

再构建邮编表并注册：

```python
RawZipRDD = sc.textFile("../data/Zipssortedbycitystate.csv")
ZipRDD = RawZipRDD.map(lambda line: line.split(","))
Zip_Rows = ZipRDD.map(lambda p: Row(CITY='"'+p[0]+'"', STATE='"'+p[1]+'"', POSTALCODE='"'+p[2]+'"')).distinct()
ZIP_df = sqlContext.createDataFrame(Zip_Rows)
ZIP_df.registerTempTable("zip_table")
```

> 注意课件这里手工给值加了一对引号 `'"'+p[0]+'"'`：因为销售表是带引号的 CSV，`split(",")` 后值本身含引号（`"NYC"`），而邮编表不带引号。两边取值形式必须一致，`join` 才连得上。

两种连接写法：

```python
# DataFrame
joined_df = sale_df.join(ZIP_df, sale_df.CITY == ZIP_df.CITY, "left_outer")
joined_df.filter('ORDERNUMBER = "10159"').distinct().show()

# Spark SQL：给两表起别名 s / z
sqlContext.sql("select s.*, z.POSTALCODE from sale_table s left join zip_table z \
                on s.CITY = z.CITY where s.ORDERNUMBER = 10159").show()
```

连接方式（`how`）常见四种：`inner`（默认，只留两边都有的）、`left_outer`（左表全留，右表补 NULL）、`right_outer`、`full_outer`，Spark 还支持 `left_semi` / `anti`。

### 3.7 数据绘图

Spark 自己不出图，链路是 **Spark 聚合 → toPandas → matplotlib**：

```python
GroupByState_df = sale_df.groupBy('STATE').count()
GroupByState_pandas_df = GroupByState_df.toPandas().set_index('STATE')

import matplotlib.pyplot as plt
%matplotlib inline
ax = GroupByState_pandas_df['count'].plot(kind='bar', title='state', figsize=(12,6), legend=True, fontsize=12)
plt.show()
```

| 程序代码 | 说明 |
| --- | --- |
| `import matplotlib.pyplot as plt` | 导入绘图模块 |
| `%matplotlib inline` | 把图形直接显示在 IPython Notebook 里 |
| `plot(kind='bar', title='state', figsize=(12,6), legend=True, fontsize=12)` | 画直方图、标题 state、图幅 12×6、显示图例、字号 12 |
| `plt.show()` | 开始绘图 |

**必须先 groupBy 聚合再 toPandas**：`toPandas()` 会把整个分布式数据集拉到 driver 单机内存，直接对 2823 行甚至上亿行原表调用会 OOM。

---

## 4. 三个易混概念

| 对象 | 归属 | 作用 |
| --- | --- | --- |
| `SparkContext`（sc） | Spark Core | 管 RDD、读文本文件、提交任务 |
| `SparkSession`（spark） | Spark SQL 入口 | 统一了老的 `SQLContext` / `HiveContext`，管 DataFrame 和 SQL |
| `sqlContext` | 老变量名 | Spark 1.x/2.x 时代的入口，现在等价于 `SparkSession` 实例 |

`sc.textFile()` 出来的是 **RDD**；`spark.read.csv()` 和 `spark.createDataFrame()` 出来的是 **DataFrame**。

---

## 5. 本机实测说明（和课件不一致的地方）

跑代码时这几处差异要先知道，不然会以为是自己写错了：

1. **数据量**：课件 `sales_data.csv` 是 2823 行，本机 `data/sales_data.csv` 只有 **5 行数据 + 1 行表头**。所以 `count()` 是 6 不是 2823，邮编为空的记录是 1 条不是 901 条。
2. **表头必须过滤**：本机这份 csv 第一行是列名，课件那份没有。不 `filter` 掉的话，表头会变成一条数据混进结果 —— `distinct()` 里会多出一个 `PRODUCTCODE` 值，`(2018 - YEAR_ID)` 会算出 `NULL`。
3. **`sqlContext` 未定义**：课件代码里的 `sqlContext` 在新版 PySpark 中不是全局变量，笔记本里用 `sqlContext = spark` 起别名，课件语句即可原样运行。
4. **`registerTempTable` 已废弃**：Spark 2.0 起标记废弃，运行会打 `FutureWarning`，但仍能跑通；现行写法是 `createOrReplaceTempView()`。
5. **缺 `Zipssortedbycitystate.csv`**：本机没有这份城市邮编表，笔记本改为**从 `sales_data.csv` 里抽 `CITY/STATE/POSTALCODE` 去重派生 zip 表**，`join` 的写法本身与课件完全一致；代价是补不出课件里 `San Francisco → 94102` 这个效果（派生表里该城市邮编本来就空）。
6. **环境**：本机 PySpark 3.5.9 + JDK 21（`JAVA_HOME=D:\jdk-21`，注意 PATH 上默认是 Java 8，需在代码里显式设 `JAVA_HOME`）。

---

## 6. 一页速查

```python
spark = SparkSession.builder.appName("x").getOrCreate()
sc, sqlContext = spark.sparkContext, spark

df = spark.createDataFrame(rdd_or_list, ["col1", "col2"])
df.printSchema(); df.show(5)

df.registerTempTable("t")                      # 或 createOrReplaceTempView("t")
spark.sql("select count(*) from t").show()

df.select("a", (2018 - df.b).alias("diff"))    # 字段计算
df.filter("b = '2003'")                        # 条件查询
df.orderBy("b")                                # 排序
df.select("a").distinct()                      # 去重
df.groupby("a").count()                        # 分组统计
df.join(other, df.CITY == other.CITY, "left_outer")   # 连接
df.groupby('S').count().toPandas().plot(kind='bar')   # 绘图
```
