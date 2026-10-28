from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder \
    .appName("SparkStreaming") \
    .master("local[2]") \
    .getOrCreate()

# 创建 StreamingContext
sc = spark.sparkContext
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 5)

# 创建DStream
# textFileStream:监听文件夹
lines = ssc.textFileStream("file:///C:/Users/lwr/Desktop/Object/spark/data/SparkStreaming/")

# Transformation
# map:逐个函数转换
words = lines.map(lambda x : (x, 1))

# flatmap；扁平化
words = lines.flatMap(lambda line: line.split(","))
words.pprint()

# filter:过滤
counts = lines.filter(lambda x: '张' in x)
counts.pprint()

# count：获取新增文件内容总行数
counts = lines.count()
counts.pprint()

# 5. 启动流式计算
ssc.start()

# 6. 等待运行结束
ssc.awaitTermination()