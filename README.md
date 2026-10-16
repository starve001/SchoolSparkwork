# Spark 大数据技术与应用（课程作业）

本仓库整理 Spark 课程的实验代码与章节知识点笔记，内容涵盖 **RDD 基础 → 共享变量 → 依赖与持久化 → DataFrame → Spark SQL → 数据连接** 的完整学习路径。

---

## 目录结构

```
.
├── README.md                     # 本文件（总索引）
├── requirements.txt              # 依赖清单
├── notebooks/                    # 实验代码（Jupyter Notebook）
│   ├── 01_RDD基础操作.ipynb
│   ├── 02_RDD转换与累加器.ipynb
│   ├── 03_共享变量与持久化.ipynb
│   ├── 04_RDD转DataFrame.ipynb
│   ├── 05_销售数据Schema与SparkSQL.ipynb
│   ├── 06_CSV转临时表与数据连接.ipynb
│   ├── 07_Pandas索引基础.ipynb
│   └── 08_第5章_DataFrame与SparkSQL.ipynb
├── scripts/                      # 独立可运行的 .py 脚本
│   ├── hello.py
│   ├── rdd_basics.py
│   ├── rdd_practice.py
│   ├── rdd_accumulator.py
│   ├── shared_variables.py
│   ├── rdd_to_dataframe.py
│   ├── sales_schema_demo.py
│   ├── sparksql_ops.py
│   ├── wordcount.py
│   └── spark_env_check.py
├── notes/                        # 章节知识点笔记（Markdown）
│   ├── 01_RDD基础操作.md
│   ├── ...
│   └── 08_第5章_DataFrame与SparkSQL.md
└── data/                         # 实验数据集
    ├── sales_data.csv
    ├── Zipssortedbycitystate.csv
    ├── score.txt
    └── article.txt
```

---

## 章节导航

| # | 章节 | 代码 | 知识点笔记 | 核心内容 |
| --- | --- | --- | --- | --- |
| 1 | RDD 基础操作 | [01](notebooks/01_RDD基础操作.ipynb) | [01](notes/01_RDD基础操作.md) | SparkSession、`parallelize` / `textFile`、map / flatMap / filter / union / intersection、reduceByKey 成绩统计 |
| 2 | RDD 转换与累加器 | [02](notebooks/02_RDD转换与累加器.ipynb) | [02](notes/02_RDD转换与累加器.md) | 累加器 `add()`、广播变量、RDD 不存储数据 |
| 3 | 共享变量与持久化 | [03](notebooks/03_共享变量与持久化.ipynb) | [03](notes/03_共享变量与持久化.md) | 累加器 / 广播变量、窄依赖与宽依赖、DAG 与 stage 划分、`persist` / `cache` |
| 4 | RDD 转 DataFrame | [04](notebooks/04_RDD转DataFrame.ipynb) | [04](notes/04_RDD转DataFrame.md) | `toDF`、`createDataFrame`、Pandas 互转、Row + StructType、词频统计 |
| 5 | 销售数据 Schema 与 Spark SQL | [05](notebooks/05_销售数据Schema与SparkSQL.ipynb) | [05](notes/05_销售数据Schema与SparkSQL.md) | 显式 StructType、CSV 表头处理、注册临时表、字段计算与隐式类型转换 |
| 6 | CSV 转临时表与数据连接 | [06](notebooks/06_CSV转临时表与数据连接.ipynb) | [06](notes/06_CSV转临时表与数据连接.md) | `spark.read.csv`、邮编表、六种 join 方式、`toPandas` 绘图、Pandas 索引 |
| 7 | Pandas 索引基础 | [07](notebooks/07_Pandas索引基础.ipynb) | [07](notes/07_Pandas索引基础.md) | `loc` / `iloc` 区别、`set_index` 与 `inplace` 坑点 |
| 8 | 第 5 章 DataFrame 与 Spark SQL | [08](notebooks/08_第5章_DataFrame与SparkSQL.ipynb) | [08](notes/08_第5章_DataFrame与SparkSQL.md) | 课件完整实操：DataFrame 创建、Spark SQL 创建、七项常用操作（双写法对照） |

---

## 数据集说明

| 文件 | 规模 | 用途 |
| --- | --- | --- |
| `data/sales_data.csv` | 5 行数据 + 表头 | 销售信息表（24 个字段），用于 Schema、Spark SQL、join |
| `data/Zipssortedbycitystate.csv` | 21379 行 | 美国城市邮编表（CITY / STATE / POSTALCODE），用于 join |
| `data/score.txt` | 6 行 | 学生成绩（姓名, 科目, 分数），用于 reduceByKey / sortBy |
| `data/article.txt` | 日志文本 | 词频统计输入（flatMap + reduceByKey） |

---

## 环境与运行

```bash
pip install -r requirements.txt
```

**依赖**：PySpark 3.5.x、JDK 8/11/17/21、Pandas、Matplotlib。

### 运行 notebook

Notebook 内的数据路径使用相对路径 `../data/xxx.csv`，因此请**从 `notebooks/` 目录下**打开并运行。

### 运行脚本

```bash
cd scripts
python rdd_basics.py
python sales_schema_demo.py
```

脚本同样以 `../data/` 为数据目录，请先 `cd scripts` 再执行。

### Windows 环境提示

在 Windows 上本地运行 PySpark 常见两个问题：

1. **找不到 `winutils.exe`** —— 属于 WARN 级别提示，本地模式不影响功能；
2. **`PYSPARK_PYTHON` 不一致** —— Driver 与 Worker 用了不同解释器会报错。脚本里已统一处理：

```python
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
```

> 这些设置**必须在 `import pyspark` 之前**执行。

---

## 学习路线建议

```
① RDD 编程基础        →  第 1、2 章（算子、累加器、广播变量）
        ↓
② RDD 运行原理        →  第 3 章（窄/宽依赖、DAG、stage、持久化）
        ↓
③ 结构化数据          →  第 4、5 章（DataFrame、Schema、Spark SQL）
        ↓
④ 综合应用            →  第 6 章（CSV 入表、join、可视化）
        ↓
⑤ 课件完整实操        →  第 8 章（第 5 章七项操作双写法对照）
```
