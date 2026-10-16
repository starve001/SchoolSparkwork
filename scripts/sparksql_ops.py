from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
import pandas as pd
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, DoubleType


def StructTypeDF():
    # 1. 把普通元组 RDD 映射成 Row RDD 指定 StructType Schema，生产最推荐
    row_rdd = rdd.map(lambda x : Row(name=x[0], age=x[1]))
    schema = StructType([
        StructField("word", StringType(), True),
        StructField("count", IntegerType(), True),
    ])
    wordCounts = spark.createDataFrame(row_rdd, schema)
    wordCounts.show()

# dict_rdd = rdd.map(lambda x : {"word" : x[0], "count" : x[1]})
# wordCounts = spark.createDataFrame(dict_rdd)
# wordCounts.show()

def pandasDF2SparkDF():
    pandasDF = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 35, 45],
        "city": ["New York", "Beijing", "Shanghai"],
    })

    sparkDF = spark.createDataFrame(pandasDF)
    sparkDF.printSchema()
    sparkDF.show()

def testLocal2Df(sparkBuilder):
    # 将本地数据容器转变为DataFrame
    data = [("Alice", 21), ("Bob", 24)]
    people = sparkBuilder.createDataFrame(data, ["name", "age"])
    people.show()


def rawSalesDataRDD():
    RawSalesDataRDD = sc.textFile("../data/sales_data.csv")

    # 过滤掉表头行，然后切分
    salesRDD = RawSalesDataRDD \
        .filter(lambda line: not line.startswith("ORDERNUMBER")) \
        .map(lambda line: line.split(","))

    saleRows = salesRDD.map(lambda p: Row(
        int(p[0]),
        int(p[1]),
        float(p[2]),
        int(p[3]),
        float(p[4]),
        p[5].strip('"'),
        p[6].strip('"'),
        int(p[7]),
        int(p[8]),
        int(p[9]),
        p[10].strip('"'),
        int(p[11]),
        p[12].strip('"'),
        p[13].strip('"'),
        p[14].strip('"'),
        p[15].strip('"'),
        p[16].strip('"') if len(p) > 16 and p[16] else None,
        p[17].strip('"') if len(p) > 17 else None,
        p[18].strip('"') if len(p) > 18 and p[18] else None,
        p[19].strip('"') if len(p) > 19 and p[19] else None,
        p[20].strip('"') if len(p) > 20 else None,
        p[21].strip('"') if len(p) > 21 else None,
        p[22].strip('"') if len(p) > 22 else None,
        p[23].strip('"') if len(p) > 23 else None
    ))

    schema = StructType([
        StructField("ORDERNUMBER", IntegerType(), nullable=True),     # 订单编号
        StructField("QUANTITYORDERED", IntegerType(), nullable=True), # 订购数量
        StructField("PRICEEACH", DoubleType(), nullable=True),        # 商品单价
        StructField("ORDERLINENUMBER", IntegerType(), nullable=True), # 订单行号
        StructField("SALES", DoubleType(), nullable=True),            # 销售金额
        StructField("ORDERDATE", StringType(), nullable=True),        # 订单日期
        StructField("STATUS", StringType(), nullable=True),           # 订单状态
        StructField("QTR_ID", IntegerType(), nullable=True),          # 季度编号
        StructField("MONTH_ID", IntegerType(), nullable=True),        # 月份编号
        StructField("YEAR_ID", IntegerType(), nullable=True),         # 年份
        StructField("PRODUCTLINE", StringType(), nullable=True),      # 产品类别
        StructField("MSRP", IntegerType(), nullable=True),            # 建议零售价
        StructField("PRODUCTCODE", StringType(), nullable=True),      # 产品编号
        StructField("CUSTOMERNAME", StringType(), nullable=True),     # 客户名称
        StructField("PHONE", StringType(), nullable=True),            # 联系电话
        StructField("ADDRESSLINE1", StringType(), nullable=True),     # 地址第一行
        StructField("ADDRESSLINE2", StringType(), nullable=True),     # 地址第二行
        StructField("CITY", StringType(), nullable=True),             # 城市
        StructField("STATE", StringType(), nullable=True),            # 州/省
        StructField("POSTALCODE", StringType(), nullable=True),       # 邮政编码
        StructField("COUNTRY", StringType(), nullable=True),          # 国家
        StructField("TERRITORY", StringType(), nullable=True),        # 销售区域
        StructField("CONTACTLASTNAME", StringType(), nullable=True),  # 联系人姓氏
        StructField("CONTACTFIRSTNAME", StringType(), nullable=True)  # 联系人名字


    ])

    # RDD -> DataFrame
    sale_df = spark.createDataFrame(saleRows, schema=schema)

    # 缓存 DataFrame
    sale_df.cache()

    # 完整执行一次计算，避免 show() 提前结束 Python Worker
    sale_df.count()

    # 查看表结构
    sale_df.printSchema()

    # 注册临时视图
    sale_df.createOrReplaceTempView("sale_table")

    # spark.sql("SELECT ORDERNUMBER, QUANTITYORDERED FROM sale_table LIMIT 5").show()
    #
    # spark.sql("SELECT COUNT(*) AS datacount FROM sale_table").show()
    #
    # sale_df.select("ORDERNUMBER", "PRODUCTCODE", (2018 - sale_df.YEAR_ID)).show(5)
    # spark.sql("select ORDERNUMBER, PRODUCTCODE, (2018 - YEAR_ID) from sale_table").show(5)

    # 使用DataFrame进行数据的筛选
    # sale_df.filter("'YEAR_ID' = '2003'").show(5)
    # 使用Sparksql筛选数据
    # spark.sql("select * from sale_table where YEAR_ID = 2003").show(5)

    # 使用 DataFrame 对数据进行排序
    # sale_df.select("ORDERNUMBER", "PRODUCTCODE", (2018 - sale_df.YEAR_ID)).orderBy("YEAR_ID").show(5)
    # 使用 Spark sql 对数据进行排序
    # spark.sql("select ORDERNUMBER, PRODUCTCODE, (2018 - YEAR_ID) from sale_table order by YEAR_ID").show(5)

    #数据去重
    #DataFrames
    # sale_df.select("YEAR_ID").distinct().show(5)
    # Spark sql
    # spark.sql("select distinct YEAR_ID from sale_table").show(5)

    # 数据分组统计
    # sale_df.select("PRODUCTCODE").groupBy("PRODUCTCODE").count().show()
    # spark.sql("select PRODUCTCODE, count(*) counts from sale_table group by PRODUCTCODE").show()

    # 1. textFile 加载 csv
    rawZipTableRDD = sc.textFile("../data/Zipssortedbycitystate.csv")

    # 2. rdd -> row
    zipTableRDD = rawZipTableRDD \
        .filter(lambda line: not line.startswith("CITY")) \
        .map(lambda line: line.split(","))

    # 3. rdd -> dataframe(指定schema信息)
    zipTableRows = zipTableRDD.map(lambda p: Row(
        CITY=p[0],
        STATE=p[1],
        POSTALCODE=p[2]

    ))
    schema = StructType([
        StructField("CITY", StringType(), nullable=True),      # 城市名称
        StructField("STATE", StringType(), nullable=True),     # 州名称
        StructField("POSTALCODE", StringType(), nullable=True) # 邮政编号

    ])

    # RDD -> DataFrame
    zip_df = spark.createDataFrame(zipTableRows, schema)

    # 缓存 DataFrame
    zip_df.cache()

    # 先完整执行一次，避免 show() 只取得前 20 行后提前断开 Worker
    zip_df.count()

    # 注册临时视图
    zip_df.createOrReplaceTempView("zip_table")

    # 查询表中全部信息（默认只显示前 20 行）
    spark.sql("select * from zip_table").show()

    # 使用DataFrames数据连接
    joined_df = sale_df.join(zip_df, sale_df.CITY == zip_df.CITY, how="left_outer")
    joined_df.filter("ORDERNUMBER = 10159").distinct().show()

    spark.spl("select s.*")

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("sparkSql") \
        .master("local[2]") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # 将普通 RDD 转换成 DataFrame
    rdd = sc.textFile("../data/article.txt") \
        .flatMap(lambda line: line.split()) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda x, y: x + y)

    # wordCounts = rdd.toDF(["word", "count"])
    # wordCounts.printSchema()

    # wordCounts = spark.createDataFrame(rdd, ["word", "count"])
    # wordCounts.show()
    # wordCounts.printSchema()

    # list = [("word", 1, ("word1", 2))]
    # rdd = sc.parallelize(list)
    # rdd.toDF(["word", "count"]).show()

    # testLocal2Df(sparkBuilder)

    rawSalesDataRDD()

    spark.stop()