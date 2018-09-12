from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CommonMovie")
sc = SparkContext(conf = conf)

rdd = sc.textFile("u.data")
onlyMovieIds = rdd.map(lambda x: (int(x.split()[1]), 1))
countPerMovie = onlyMovieIds.reduceByKey(lambda x,y : x+y)

reverseRdd = countPerMovie.map(lambda (x,y) : (y,x))
sortedResult = reverseRdd.sortByKey()

results = sortedResult.collect()

for result in results:
	print(result)
