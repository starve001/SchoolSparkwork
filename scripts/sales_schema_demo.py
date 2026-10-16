"""本地运行 sales_data.csv 的 PySpark 示例。"""

import csv
import os
import sys
from pathlib import Path

# 必须在导入 pyspark 前设置，确保 Driver 和 Worker 使用同一解释器。
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


BASE_DIR = Path(__file__).resolve().parent
SALES_DATA_PATH = BASE_DIR.parent / "data" / "sales_data.csv"

SCHEMA = StructType([
    StructField("ORDERNUMBER", IntegerType(), True),
    StructField("QUANTITYORDERED", IntegerType(), True),
    StructField("PRICEEACH", DoubleType(), True),
    StructField("ORDERLINENUMBER", IntegerType(), True),
    StructField("SALES", DoubleType(), True),
    StructField("ORDERDATE", StringType(), True),
    StructField("STATUS", StringType(), True),
    StructField("QTR_ID", IntegerType(), True),
    StructField("MONTH_ID", IntegerType(), True),
    StructField("YEAR_ID", IntegerType(), True),
    StructField("PRODUCTLINE", StringType(), True),
    StructField("MSRP", IntegerType(), True),
    StructField("PRODUCTCODE", StringType(), True),
    StructField("CUSTOMERNAME", StringType(), True),
    StructField("PHONE", StringType(), True),
    StructField("ADDRESSLINE1", StringType(), True),
    StructField("ADDRESSLINE2", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("POSTALCODE", StringType(), True),
    StructField("COUNTRY", StringType(), True),
    StructField("TERRITORY", StringType(), True),
    StructField("CONTACTLASTNAME", StringType(), True),
    StructField("CONTACTFIRSTNAME", StringType(), True),
])


def parse_sale(line: str) -> Row:
    fields = next(csv.reader([line]))
    fields.extend([None] * (24 - len(fields)))
    return Row(
        int(fields[0]), int(fields[1]), float(fields[2]), int(fields[3]), float(fields[4]),
        fields[5], fields[6], int(fields[7]), int(fields[8]), int(fields[9]),
        fields[10], int(fields[11]), fields[12], fields[13], fields[14], fields[15],
        fields[16] or None, fields[17] or None, fields[18] or None, fields[19] or None,
        fields[20] or None, fields[21] or None, fields[22] or None, fields[23] or None,
    )


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("SalesData1013").master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def raw_sales_data_rdd(spark: SparkSession):
    if not SALES_DATA_PATH.is_file():
        raise FileNotFoundError(f"找不到数据文件：{SALES_DATA_PATH}")

    sale_rows = (
        spark.sparkContext.textFile(SALES_DATA_PATH.as_uri())
        .filter(lambda line: not line.startswith("ORDERNUMBER"))
        .filter(lambda line: line.strip())
        .map(parse_sale)
    )
    sale_df = spark.createDataFrame(sale_rows, schema=SCHEMA)
    sale_df.printSchema()
    sale_df.show(5, truncate=False)
    sale_df.createOrReplaceTempView("sale_table")
    spark.sql("SELECT ORDERNUMBER, QUANTITYORDERED FROM sale_table LIMIT 5").show()
    spark.sql("SELECT COUNT(*) AS datacount FROM sale_table").show()
    sale_df.select("ORDERNUMBER", "PRODUCTCODE", (2018 - sale_df.YEAR_ID).alias("YEARS_SINCE_2018")).show(5)
    return sale_df


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(f"Driver / Worker Python: {sys.executable}")
        raw_sales_data_rdd(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
