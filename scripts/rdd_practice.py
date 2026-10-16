from pyspark.sql import SparkSession

sparkBuilder = SparkSession.builder \
    .appName("rddDemo") \
    .master("local[2]") \
    .config("spark.log.level", "WARN") \
    .getOrCreate()

sc = sparkBuilder.sparkContext

# TODO 创建1-10的RDD，使用map将所有元素 *2
rdd = sc.parallelize(range(1, 11))
print(rdd.collect())
rdd1 = rdd.map(lambda x : x * 2).collect()
print(rdd1)

# TODO 创建10-20的RDD，使用mapPartitions将所有元素 *2
rdd = sc.parallelize(range(10, 21))
print(rdd.collect())
rdd2 = rdd.mapPartitions(lambda iter : [x * 2 for x in iter])
print(rdd2.collect())

# TODO 过滤出包含"xiao"子串的元素
name = ['xiaoli', 'laoli', 'laowang', 'xiaoming', 'xiaojing', 'xiaokong']
rdd = sc.parallelize(name)
print(rdd.collect())
# filter:按条件筛选元素，保留满足条件的，丢弃不满足条件的
rdd4 = rdd.filter(lambda x: "xiao" in x).collect()
print(rdd4)

# TODO 对RDD中的元素执行去重操作
list = [10, 10, 2, 5, 3, 5, 3, 6, 9, 1]
rdd = sc.parallelize(list)
print(rdd.collect())
rdd5 = rdd.distinct().collect()
print(rdd5)

# TODO 创建两个RDD，求并集和交集
rdd6 = sc.parallelize([1, 2, 3, 3, 4])
rdd7 = sc.parallelize([3, 4, 5, 5, 6])
print(rdd6.collect())
print(rdd7.collect())

# 并集
rdd8 = rdd7.union(rdd6).distinct()
print(rdd8.collect())

# 交集
rdd8 = rdd7.intersection(rdd6)
print(rdd8.collect())

# 持久化
rdd8.persist()
rdd8.cache()

# TODO 1.读取文件转换成（姓名，分数）格式
rdd_file = sc.textFile("../data/score.txt")
rdd_data = rdd_file.map(lambda line: line.split(",")) \
    .map(lambda x: (x[0], float(x[2])))

print(rdd_data.collect())

# TODO 计算每个学生的总分数
rdd_sum = rdd_data.reduceByKey(lambda a, b: a + b)
print(rdd_sum.collect())

# TODO 总分降序排序
rdd_sort = rdd_sum.sortBy(lambda x: x[1], ascending=False)
print(rdd_sort.collect())

