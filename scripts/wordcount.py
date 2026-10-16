#导入SparkContext对象
from pyspark.sql import SparkSession

sparkBuilder = SparkSession.builder\
    .appName("rddDemo")\
    .master("local[1]")\
    .getOrCreate()
sc = sparkBuilder.sparkContext
#配置SparkContext
#创建一个新的RDD,加载本地文件
lines = sc.textFile("../data/article.txt")
#flatMap(func):与map()相似,但每个输入元素都可以映射到0或多个输出结果
#map(func):将每个元素传递到函数func中,并将结果返回为一个新的数据集
#reduceByKey(func):应用于(K,V)键值对的数据集时,返回一个新的(K, V)形式的数据集,其中的每个值是将每个key传递到函数func中进行聚合
result = lines.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
print (result.collect())