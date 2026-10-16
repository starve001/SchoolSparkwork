"""RDD 转换与累加器示例，可直接在 Windows 本地运行。"""

import os
import sys

# 必须在导入 pyspark 前设置，避免 Windows 上默认寻找不存在的 ``python3``。
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("RddDemo").master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    try:
        counter = sc.accumulator(0)
        rdd = sc.parallelize(range(10))

        def increment(value: int) -> None:
            counter.add(value)

        rdd.foreach(increment)
        print(f"Driver / Worker Python: {sys.executable}")
        print("Counter value:", counter.value)  # 0 + 1 + ... + 9 = 45
        print("RDD value:", rdd.collect())
        print("hello world")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
