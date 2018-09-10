from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("fakefriends.csv")
rdd = lines.map(parseLine)
totalValues = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
avgValues = totalValues.mapValues(lambda x: x[0]/x[1])
results = avgValues.collect()

for result in results:
    print(result)
