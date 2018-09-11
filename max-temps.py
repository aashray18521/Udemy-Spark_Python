from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("maxTemp")
sc = SparkContext(conf = conf)

def parseLine(line):
	value = line.split(",")
	stationId = value[0]
	entryType = value[2]
	temp = value[3]
	return (stationId, entryType, temp)

rdd = sc.textFile("1800.csv")
parsedLines = rdd.map(parseLine)
maxOnly = parsedLines.filter(lambda x: "TMAX" in x[1])
sortedVals = maxOnly.map(lambda x: (x[0], x[2]))
parsedLines = sortedVals.reduceByKey(lambda x,y: max(x,y))
results = parsedLines.collect()

for result in results:
	print(result[0] + "\t" + result[1] + "C")
print("\n")
print(results)
print("\n")
