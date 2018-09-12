from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularSuperHeroGraph")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
	elements = line.split()
	return (int(elements[0]), len(elements)-1)

def parseNames(line):
	fields = line.split('\"')
	return (int(fields[0]), fields[1].encode('utf8'))

names = sc.textFile("Marvel-names.txt")
namesRdd = names.map(parseNames)

#print(namesRdd.collect())

one = namesRdd.lookup(13)
two = one[0]

# str is not required for 'two'
print("\n\n\n\n\n\n\n\n\n\n" + str(one) + "\n\n\n\n\n\n\n\n\n\n\n\n\n" + str(two))

lines = sc.textFile("Marvel-graph.txt")

pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x,y: x+y)
flipped = totalFriendsByCharacter.map(lambda (x,y): (y,x))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print("\n\n\n\n" + mostPopularName + " is the most popular superhero, with " +str(mostPopular[0]) + " co-appearences.\n\n\n\n")
