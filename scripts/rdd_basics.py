# ========== 修改为你本机真实路径 ==========

#os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\MINBOOK\python.exe

from pyspark.sql import SparkSession

sparkBuilder = SparkSession.builder\
    .appName("rddDemo")\
    .master("local[1]")\
    .getOrCreate()
sc = sparkBuilder.sparkContext
#创建一个1-10数组的RDD，将所有元素*2形成新的Rdd map
#data
rdd = sc.parallelize(range(1,11))
print(rdd.collect())
rdd1 = rdd.map(lambda x: x*2).collect()
print(rdd1)
#Todd 创建10-20的RDD，使用mapPartitions将所有元素*2
range(10,21)
rdd = sc.parallelize(range(10,21))
print(rdd.collect())
rdd1 = rdd.mapPartitions(lambda iter:[x*2 for x in iter])
print(rdd1.collect())
#
rdd1 = sc.parallelize( [1,2,3,4,5])
rdd2 = sc.parallelize([4,5,6,7,8])
rdd3 = rdd1.union(rdd2)
print(rdd3.collect())
rdd4 = rdd1.intersection(rdd2)
print(rdd4.collect())
#1.将数据转换成（姓名，分数）格式
sc.textFile("../data/score.txt")
data = [
    "张三,语文,88",
    "张三,数学,92",
    "李四,语文,78",
    "李四,数学,85",
    "王五,语文,90",
    "王五,数学,76"
]
rdd = sc.parallelize(data)

# map转换：分割字符串，取第0个(姓名)和第2个(分数)，分数转为int
result_rdd = rdd.map(lambda line: line.split(",")) \
                .map(lambda arr: (arr[0], int(arr[2])))

# 查看结果
print(result_rdd.collect())
#2.计算每个学生总分数

#3.总分降序

