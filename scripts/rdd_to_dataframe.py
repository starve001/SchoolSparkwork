from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
import pandas as pd


# 创建 SparkSession
def create_spark():
    spark = SparkSession.builder \
        .appName("sparkSql") \
        .master("local[2]") \
        .config("spark.log.level", "WARN") \
        .getOrCreate()

    return spark

# 读取文件并创建词频统计 RDD
def create_word_count_rdd(sc):
    rdd = sc.textFile("../data/article.txt") \
        .flatMap(lambda line: line.split()) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda x, y: x + y)

    return rdd


# 方法一：使用 toDF() 将 RDD 转换成 DataFrame
def rdd_to_df_by_toDF(rdd):
    word_counts = rdd.toDF(["word", "count"])

    word_counts.show()
    word_counts.printSchema()

    return word_counts

# 方法二：使用 createDataFrame() 将 RDD 转成 DataFrame
def rdd_to_df_by_createDataFrame(spark, rdd):
    word_counts = spark.createDataFrame(
        rdd,
        ["word", "count"]
    )

    word_counts.show()
    word_counts.printSchema()

    return word_counts

# 方法三：RDD → Row RDD → StructType → DataFrame
def rdd_to_df_by_struct_type(spark, rdd):
    # 把普通元组 RDD 映射成 Row RDD
    row_rdd = rdd.map(
        lambda x: Row(
            word=x[0],
            count=x[1]
        )
    )

    # 手动指定 Schema
    schema = StructType([
        StructField("word", StringType(), True),
        StructField("count", IntegerType(), True)
    ])

    # 创建 DataFrame
    word_counts = spark.createDataFrame(
        row_rdd,
        schema
    )

    word_counts.show()
    word_counts.printSchema()

    return word_counts

# 方法四：本地 List → RDD → DataFrame
def list_to_rdd_to_df(sc):
    data = [
        ("word", 1),
        ("word1", 2)
    ]

    rdd = sc.parallelize(data)

    word_counts = rdd.toDF(
        ["word", "count"]
    )

    word_counts.show()
    word_counts.printSchema()

    return word_counts

# Pandas DataFrame → Spark DataFrame
def pandas_df_to_spark_df(spark):
    pandas_df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 35, 45],
        "city": ["New York", "Beijing", "Shanghai"]
    })

    spark_df = spark.createDataFrame(pandas_df)

    spark_df.show()
    spark_df.printSchema()

    return spark_df

# 本地数据直接创建 DataFrame
def local_data_to_df(spark):
    data = [
        ("Alice", 21),
        ("Bob", 24)
    ]

    people = spark.createDataFrame(
        data,
        ["name", "age"]
    )

    people.show()
    people.printSchema()

    return people

# 主函数
def main():
    # 创建 SparkSession
    spark = create_spark()

    # 获取 SparkContext
    sc = spark.sparkContext

    # 创建词频 RDD
    rdd = create_word_count_rdd(sc)

    # --------------------------------------------------------
    # 根据需要调用不同的方法
    # --------------------------------------------------------

    # 方法一
    # rdd_to_df_by_toDF(rdd)

    # 方法二
    # rdd_to_df_by_createDataFrame(spark, rdd)

    # 方法三
    # rdd_to_df_by_struct_type(spark, rdd)

    # 方法四
    # list_to_rdd_to_df(sc)

    # Pandas DataFrame 转 Spark DataFrame
    # pandas_df_to_spark_df(spark)

    # 本地数据直接创建 DataFrame
    local_data_to_df(spark)

    # 停止 Spark
    spark.stop()


# ============================================================
# 程序入口
# ============================================================
if __name__ == '__main__':
    main()