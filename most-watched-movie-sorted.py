from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CommonMovie")
sc = SparkContext(conf = conf)

def onlyId(lines):
	dataList = lines.split()
	movieId = dataList[1]
	return (movieId)

rdd = sc.textFile("u.data")
onlyMovieIds = rdd.map(onlyId)
axisAdd = onlyMovieIds.map(lambda x: (x,1))
countPerMovie = axisAdd.reduceByKey(lambda x,y : x+y)
reverseRdd = countPerMovie.map(lambda (x,y) : (y,x))
sortedResult = reverseRdd.sortByKey()

results = sortedResult.collect()

for result in results:
	print(result)
