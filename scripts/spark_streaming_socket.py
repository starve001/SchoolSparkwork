from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel

spark = SparkSession.builder \
    .appName("SparkStreaming") \
    .master("local[2]") \
    .getOrCreate()
# 创建 StreamingContext
sc = spark.sparkContext
ssc = StreamingContext(sc, 10)
ds = ssc.socketTextStream("localhost", 8888, StorageLevel.DISK_ONLY_3)
# ds.pprint(30)
# ds1 = ds.flatMap(lambda line: line.split(" "))
# ds2 = ds1.map(lambda line: line+ "one")
# ds3 = ds1.map(lambda line: line+ "two")
# ds_union = ds2.union(ds3)
# ds2.pprint()
# ds3.pprint()
# ds_union.pprint()
ds1 = ds.flatMap(lambda p: p.split(","))
ds1.pprint()
# ds2 = ds1.map(lambda line: (line, 1))
ds2 = ds1.flatMap(lambda arr: [(word,1)for word in arr])
ds2.pprint()
ds3 = ds2.reduceByKey(lambda x, y: x + y)
ds3.pprint()
ssc.start()
ssc.awaitTermination()
