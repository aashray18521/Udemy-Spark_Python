# Boilerplate Stuff
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

# The characters we want to find degree of separation between
startCharacterID = 5306 #SpiderMan
targetCharacterID = 859 #Captain America

# Our Accumulator, used to signal when we find our Target Character during our BFS traversal
hitCounter = sc.accumulator(0)

def convertToBFS(line):
	fields = line.split()
	heroID = int(fields[0])
	connections = []
	for connection in fields[1:]:
		connections.append(connection)

	color = 'WHITE'
	distance = 9999

	if(heroID == startCharacterID):
		color = 'GRAY'
		distance = 0

	return (heroID, (connections, distance, color))


def createStartingRdd():
	inputFile = sc.textFile("Marvel-graph.txt")
	return inputFile.map(convertToBFS)	


def bfsMap(node):
	characterID = node[0]
	data = node[1]
	connections = data[0]
	distance = data[1]
	color = data[2]
	
	results = []
	
	# If this node color needs to be expanded...
	if (color == 'GRAY'):
		for connection in connections:
			newCharacterID = connection
			newDistance = distance + 1
			newColor = 'GRAY'
			if (targetCharacterID == connection):
				hitCounter.add(1)

			newEntry = (newCharacterID, ([], newDistance, newColor))
			results.append(newEntry)
		# We've processed this node, color it BLACK
		color = 'BLACK'
	# Emit the input node so that we don't lose it.
	results.append( (characterID, (connections, distance, color)) )
	return results


def bfsReduce(data1, data2):
	edges1 = data1[0]
	edges2 = data2[0]
	distance1 = data1[1]
	distance2 = data2[1]
	color1 = data1[2]
	color2 = data2[2]
	
	distance = 9999
	color = 'WHITE'
	edges = []
	
	# See if one is the original node with its connections.
	# If so, preserve them.
	if (len(edges1) > 0):
		edges = edges1
	elif (len(edges2) > 0):
		edges = edges2
	
	# Preserve minimum ditance 
	if (distance1 < distance):
		distance = distance1
	elif(distnce2 < distance):
		distance = distance2

	# Preserve darkest color
	if(color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
		color = color2
	elif(color2 == 'GRAY' and color2 == 'BLACK'):
		color = color2
	
	return (edges, distance, color)


# MAIN CODE
iterationRdd = createStartingRdd()

for iteration in range(0,10):
	print("Running BFS iteration #" + str(iteration+1))
	
	# Create new vertices as needed to darken or reduce distances in the
	# reduce stage. If we encounter the node we're looking for as a GRAY
	# node, increment our accumulator to signal that we're DONE!!!
	mapped = iterationRdd.flatMap(bfsMap)

	# Note that mapped.count() action here forces the RDD to be evaluated, and
	# that's the only reason our accumulator is actually updated.
	print("Processing " + str(mapped.count()) + " values.")

	if(hitCounter.value > 0):
		print("Hit the target character! From " + str(hitCounter.value() + " different direction(s).")
		break

	# Reducer combines the data for each character ID, preserving the darkest color and
	# the shortest path.
	iterationRdd = mapped.reduceByKey(bfsReduce)



