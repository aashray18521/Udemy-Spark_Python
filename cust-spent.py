from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustSpent")
sc = SparkContext(conf = conf)

def reducer(line):
	fields = line.split(',')
	return (int(fields[0]), float(fields[2]))

rdd = sc.textFile("customer-orders.csv")
mappedInp = rdd.map(reducer)
values = mappedInp.reduceByKey(lambda x,y: x+y)

results = values.collect()

for result in results:
	print(result)
