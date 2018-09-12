from pyspark import SparkConf, SparkContext

def loadMovieNames():
	movieNames = {}
	with open("ml-100k/u.item") as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])] = fields[1]
	return movieNames

conf = SparkConf().setMaster("local").setAppName("nicePopularMovie")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadMovieNames())

rdd = sc.textFile("ml-100k/u.data")
onlyMovieIds = rdd.map(lambda x: (int(x.split()[1]), 1))
countPerMovie = onlyMovieIds.reduceByKey(lambda x,y : x+y)

reverseRdd = countPerMovie.map(lambda (x,y) : (y,x))
sortedMovies = reverseRdd.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda (count, movie) : (nameDict.value[movie], count))

results = sortedMoviesWithNames.collect()

for result in results:
	print(result)
