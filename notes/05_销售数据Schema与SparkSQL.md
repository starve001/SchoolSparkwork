# 第 5 章　销售数据 Schema 与 Spark SQL

> 对应 notebook：`notebooks/05_销售数据Schema与SparkSQL.ipynb`
> 对应脚本：`scripts/sales_schema_demo.py`
> 数据：`data/sales_data.csv`

---

## 一、为什么需要显式 Schema

`sc.textFile()` 读 CSV 得到的是**纯文本 RDD**，所有字段都是字符串，Spark 不知道哪列是数字、哪列是日期。

两种建 Schema 的路线：

| 路线 | 做法 | 特点 |
| --- | --- | --- |
| **推断（inferSchema）** | 让 Spark 自己猜 | 方便，但可能猜错，且要多扫一遍数据 |
| **显式声明（StructType）** | 自己写字段名 + 类型 | **生产推荐**，类型完全可控 |

---

## 二、显式定义 StructType

```python
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DecimalType, ByteType)

schema = StructType([
    StructField("ORDERNUMBER",      IntegerType(), nullable=True),
    StructField("QUANTITYORDERED",  IntegerType(), nullable=True),
    StructField("PRICEEACH",        DecimalType(), nullable=True),
    StructField("ORDERLINENUMBER",  IntegerType(), nullable=True),
    StructField("SALES",            DecimalType(), nullable=True),
    StructField("ORDERDATE",        StringType(),  nullable=True),
    StructField("STATUS",           StringType(),  nullable=True),
    StructField("QTR_ID",           ByteType(),    nullable=True),
    StructField("MONTH_ID",         ByteType(),    nullable=True),
    StructField("YEAR_ID",          IntegerType(), nullable=True),
    StructField("PRODUCTLINE",      StringType(),  nullable=True),
    StructField("MSRP",             IntegerType(), nullable=True),
    StructField("PRODUCTCODE",      StringType(),  nullable=True),
    StructField("CUSTOMERNAME",     StringType(),  nullable=True),
    StructField("PHONE",            StringType(),  nullable=True),
    StructField("ADDRESSLINE1",     StringType(),  nullable=True),
    StructField("ADDRESSLINE2",     StringType(),  nullable=True),
    StructField("CITY",             StringType(),  nullable=True),
    StructField("STATE",            StringType(),  nullable=True),
    StructField("POSTALCODE",       StringType(),  nullable=True),
    StructField("COUNTRY",          StringType(),  nullable=True),
    StructField("TERRITORY",        StringType(),  nullable=True),
    StructField("CONTACTLASTNAME",  StringType(),  nullable=True),
    StructField("CONTACTFIRSTNAME", StringType(),  nullable=True),
])
```

**StructType 的三层结构**

| 概念 | 说明 |
| --- | --- |
| `StructType` | 整个表的 Schema（字段列表） |
| `StructField` | 单个字段（名字 + 类型 + 是否可空） |
| `IntegerType` / `StringType` … | 字段的数据类型 |

---

## 三、读取 CSV 并生成 Row

```python
# 1. 读取文件生成 RDD，先按文本读取
saleRDD = sc.textFile("../data/sales_data.csv")

# 2. 去除表头第一行，再按逗号切分
header = saleRDD.first()
saleRDD = saleRDD.filter(lambda line: line != header) \
                 .map(lambda line: line.split(","))

# 3. 组装 Row 对象（同时完成类型转换）
from pyspark.sql import Row
from decimal import Decimal

sale_Rows = saleRDD.map(lambda line: Row(
    ORDERNUMBER=int(line[0]),
    QUANTITYORDERED=int(line[1]),
    PRICEEACH=Decimal(line[2]),
    ORDERLINENUMBER=int(line[3]),
    SALES=Decimal(line[4]),
    ORDERDATE=line[5],
    STATUS=line[6],
    QTR_ID=int(line[7]),
    MONTH_ID=int(line[8]),
    YEAR_ID=int(line[9]),
    PRODUCTLINE=line[10],
    MSRP=int(line[11]),
    PRODUCTCODE=line[12],
    CUSTOMERNAME=line[13],
    PHONE=line[14],
    ADDRESSLINE1=line[15],
    ADDRESSLINE2=line[16],
    CITY=line[17],
    STATE=line[18],
    POSTALCODE=line[19],
    COUNTRY=line[20],
    TERRITORY=line[21],
    CONTACTLASTNAME=line[22],
    CONTACTFIRSTNAME=line[23]
))

# 4. 用 Schema 创建 DataFrame
df = spark.createDataFrame(sale_Rows, schema=schema)
```

> **表头必须过滤**：`textFile` 不知道哪行是表头，不过滤的话表头会变成一条数据混进结果，导致 `count()` 多 1、聚合出现 `NULL`。

---

## 四、注册临时表并使用 Spark SQL

```python
df.createOrReplaceTempView("sale_table")     # 注册临时视图

spark.sql("SELECT ORDERNUMBER, QUANTITYORDERED FROM sale_table LIMIT 5").show()
spark.sql("SELECT COUNT(*) AS datacount FROM sale_table").show()
```

### 三种注册方式的区别

| 写法 | 生命周期 | 说明 |
| --- | --- | --- |
| `registerTempTable("t")` | 当前 session | 老 API，Spark 2.0 起**已废弃**，会打 `FutureWarning` |
| `createOrReplaceTempView("t")` | 当前 session | **现行推荐**，可重复创建并覆盖 |
| `createGlobalTempView("t")` | 跨 session | 用 `global_temp.表名` 访问 |

---

## 五、字段计算与 SQL 类型转换

```python
# DataFrame 写法
df.select("ORDERNUMBER", "PRODUCTCODE", (2018 - df.YEAR_ID)).show(5)

# Spark SQL 写法
spark.sql("SELECT ORDERNUMBER, PRODUCTCODE, (2018 - YEAR_ID) FROM sale_table").show(5)
```

**要点**：如果 Schema 里 `YEAR_ID` 是 `IntegerType`，结果是整数 `15`；如果是字符串（`split` 后未转型），Spark 会**隐式 CAST 成 DOUBLE**，结果显示 `15.0`。

> 这解释了为什么同一个表达式在不同 Schema 下输出格式不同 —— **根源在字段类型**，不是写法问题。

---

## 六、本章小结与考点

1. `textFile` 读 CSV 得到的是纯文本 RDD，**所有字段都是字符串**；
2. Schema 三要素：`StructType`（表）→ `StructField`（字段）→ 具体类型；
3. 生产推荐**显式 Schema**，不依赖 `inferSchema` 推断；
4. 表头行必须 `filter` 掉，否则混入数据；
5. 注册临时表推荐 `createOrReplaceTempView`，`registerTempTable` 已废弃；
6. 字段类型决定运算结果的显示格式（`15` vs `15.0`）；
7. `Row` 组装时可同时完成类型转换，是 RDD → DataFrame 的关键一步。
