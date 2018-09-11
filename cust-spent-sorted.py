from pyspark import SparkConf, SparkContext

def reducer(line):
	fields = line.split(',')
	return (int(fields[0]), float(fields[2]))

conf = SparkConf().setMaster("local").setAppName("CustSpentSorted")
sc = SparkContext(conf = conf)

rdd = sc.textFile("customer-orders.csv")
mappedValues = rdd.map(reducer)
addedSpent = mappedValues.reduceByKey(lambda x,y: x+y)
reversedRdd = addedSpent.map(lambda (x,y): (y,x))
sortedResult = reversedRdd.sortByKey()

results = sortedResult.collect()

for result in results:
	print("%i"%result[1] + "\t:\t%.2f"%result[0]) 
