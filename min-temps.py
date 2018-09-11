from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("minTemps")
sc = SparkContext(conf = conf)

def parseLine(line):
	values = line.split(',')
	stationId = values[0]
	entryType = values[2]
	temperature = values[3]
	return (stationId, entryType, temperature)

rdd = sc.textFile("1800.csv")
parsedLines = rdd.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x,y: min(x,y))
results = minTemps.collect()

for result in results:
	print result[0] + "\t" + result[1] + "C"
