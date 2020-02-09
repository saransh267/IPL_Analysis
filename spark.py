from __future__ import print_function
import re
import sys
from operator import add
from pyspark.sql import SparkSession

def func(data):
	parts = data.split(',')
	if parts[0] == 'ball':
		l = [0,0,0,0,0,0,0,0,0]
		runs = parts[7]
		if(parts[9] in ["lbw","caught","caught and bowled","bowled","stumped","hit wicket","obstructing the field"]):
			l[7] += 1
		l[int(runs)] += 1
		l[8] += 1
		key = parts[4]+','+parts[6]
		return key,l
		
def toCSVLine(data):
	key = data[0]
	l = data[1]
	s = ''
	k = 0
	for i in key.split(','):
		if k == 0:
			s+=i
			k+=1
		else:
			s+=','
			s+=i
	for j in l:
		s+=','
		s+=str(j)
	return s

def mapfunc(x):
	for i in range(len(x)-1):
		x[i] = x[i]/x[-1]
	return x
		

		
if __name__ == "__main__":
	
	spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	stats = lines.map(lambda urls: func(urls)).filter(lambda x: x is not None).reduceByKey(lambda x,y:list(map(sum,zip(x,y))))
	probs = stats.mapValues(mapfunc)
	csv = probs.map(toCSVLine)
	csv.saveAsTextFile('hdfs://localhost:9000/spark/pvp.csv')
		

