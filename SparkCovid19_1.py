import sys
import time

from pyspark import SparkContext, SparkConf
from operator import add
import datetime


def is_valid(x):
	try:
		date = datetime.datetime.strptime(x[0], "%Y-%m-%d")

		if date>=start.value and date<=end.value:
			return (x[1],int(x[3]))
	except:
		pass
	return None

 
if __name__ == "__main__":
	start_time = time.time()

	sc = SparkContext("local","spark task 2")

	min_date = datetime.datetime(2019, 12, 31)
	max_date = datetime.datetime(2020, 4, 8)


	try:
		start = sc.broadcast(datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d"))
		end = sc.broadcast(datetime.datetime.strptime(sys.argv[3], "%Y-%m-%d"))

		if end.value<start.value or start.value<min_date or max_date<end.value:
			print("Invalid dates entered in input")
			exit(1)
			
	except Exception as e:
		print("Error: invalid input")
		exit(1)

	# start = sc.broadcast(datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d"))
	# end = sc.broadcast(datetime.datetime.strptime(sys.argv[3], "%Y-%m-%d"))

	
	# read data from text file and split each line into words
	words = sc.textFile(sys.argv[1]).map(lambda line: line.split(","))
	
	# # count the occurrence of each word
	wordCounts = words.map(is_valid).filter(lambda x: x is not None).reduceByKey(add)
	
	# save the counts to output
	wordCounts.map(lambda x: x[0] + ' , ' + str(x[1])).saveAsTextFile(sys.argv[4])

	print("Time taken --- %s seconds ---" % (time.time() - start_time))