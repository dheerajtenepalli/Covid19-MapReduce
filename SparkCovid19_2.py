import sys
import time
 
from pyspark import SparkContext, SparkConf
from operator import add
from csv import reader

pop_dict={}
def is_valid(x):
	if x[2].isdigit():
		return (x[1],int(x[2]))
	else:
		return None

def divide(x):
	global pop_dict
	try:
		ret = ((x[1]*1000000.0)/broadcasted_dict.value[x[0]])
		c = x[0] + " , " + str(ret)
		# print("----",c)
		return c
	except:
		return None

 
if __name__ == "__main__":
	start_time = time.time()

	sc = SparkContext("local","Spark task 3")


	with open(sys.argv[2]) as file_ptr:
		lines = reader(file_ptr,delimiter = ',')
		for line in lines:
			# line = line.rstrip().split(",")
			# if len(line)==6:
			# 	line[1]=line[2]
			# 	line[4]=line[5]

			if line[4].isdigit():
				population = int(line[4])
				pop_dict.update({line[1] : population})

	broadcasted_dict = sc.broadcast(pop_dict)

	
	# read data from text file and split each line into words
	words = sc.textFile(sys.argv[1]).map(lambda line: line.split(","))
	
	# # count the occurrence of each word
	wordCounts = words.map(is_valid).filter(lambda x: x is not None).reduceByKey(add)

	
	# save the counts to output
	wordCounts.map(divide).filter(lambda x: x is not None).saveAsTextFile(sys.argv[3])

	print("Time taken --- %s seconds ---" % (time.time() - start_time)) 
	