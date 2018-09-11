import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
	return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCountBetter")
sc = SparkContext(conf = conf)

rdd = sc.textFile("book.txt")
words = rdd.flatMap(normalizeWords)
wordsCount = words.countByValue()

for word, count in wordsCount.items():
	cleanWord = word.encode('ascii', 'ignore')
	if(cleanWord):
		print(cleanWord, count)
