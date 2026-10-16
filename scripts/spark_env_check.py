import os
import sys

# 强制 Spark Driver 和 Worker 使用当前 PyCharm 的 Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 强制 Spark 使用本机回环地址
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SparkTest")
    .master("local[1]")  # 暂时只使用一个线程，方便排查
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.python.worker.reuse", "false")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("Python路径：", sys.executable)
print("Python版本：", sys.version)
print("PySpark Python：", os.environ["PYSPARK_PYTHON"])

rdd = sc.parallelize([1, 2, 3, 4, 5])

print("原始数据：")
print(rdd.collect())

print("map结果：")
print(rdd.map(lambda x: x * 2).collect())

spark.stop()