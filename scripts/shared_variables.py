from pyspark.sql import SparkSession
from pyspark import StorageLevel

sparkBuilder = SparkSession.builder \
    .appName("rddDemo") \
    .master("local[2]") \
    .config("spark.log.level", "WARN") \
    .getOrCreate()

sc = sparkBuilder.sparkContext


'''
counter = sc.accumulator(0)
rdd = sc.parallelize(range(10))

def increment(x):
    # global counter
    # counter += x
    counter.add(x) # 调用 Accumulator 方法，只改内部值，不会修改值类型

rdd.foreach(increment)
print("Counter value", counter.value)
'''

'''
b = sc.broadcast([1,2,3,4,5,6])
print(b.value)
b1 = sc.parallelize([0,0]).flatMap(lambda x: b.value).collect()
print(b1)
b.unpersist()
'''

# rdd = sc.parallelize(['b', 'a', 'c'])
# rdd.persist()
# rdd.persist(StorageLevel.DISK_ONLY_3)
# level = rdd.getStorageLevel()

# print("level : ", level)

# 查看RDD是否被缓存
# print("is_cached : ", rdd.is_cached)


# if rdd.is_cached:
#     level = rdd.getStorageLevel()
#     print("level.useDisk :\t\t", level.useDisk)
#     print("level.useMemory :\t", level.useMemory)
#     print("level.useOffHeap :\t", level.useOffHeap)
#     print("level.replication :\t", level.replication)

# rdd.unpersist()

# rdd = sc.paralellize([1,2,3,4,5], 5)
# rdd.saveAsTextFile("./temp")
