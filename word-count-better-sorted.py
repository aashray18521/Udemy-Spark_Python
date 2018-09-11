import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
	return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordsCountSorted")
sc = SparkContext(conf = conf)

rdd = sc.textFile("book.txt")
words = rdd.flatMap(normalizeWords)
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
wordCountsSorted = wordCounts.map(lambda (x,y): (y,x)).sortByKey()

results = wordCountsSorted.collect()

for result in results:
	count = str(result[0])
	word = result[1].encode('ascii', 'ignore')
	if(word):
		print(word, count)

