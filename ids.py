from __future__ import print_function
import re
import sys
from operator import add
from pyspark.sql import SparkSession
from pyspark import SparkContext
from math import sqrt

sc =SparkContext()
global i
i=0
global j
j=0

def mapfunc(x):
	l=x.split(',')
	return (l[0],l[1:])

def givebatid(x):
	global i
	i+=1
	return x[0],i

def givebowlid(x):
	global j
	j+=1
	return x[0],j

def addbatid(x):
	id1=[x[1][1]]
	l=x[1][0]
	return l[0],id1+l[1:]

def addbowlid(x):
	id2 = [x[1][0]]
	l=x[1][1]
	return l[:1]+id2+l[1:]

def toCSVLine(data):
	s = ''
	k = 0
	for i in data:
		if k == 0:
			s+=str(i)
			k+=1
		else:
			s+=','
			s+=str(i)
	return s
			
if __name__ == "__main__":
	
	spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

	pvp = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0]).map(mapfunc)
	bowlerid = spark.read.text(sys.argv[2]).rdd.map(lambda r: r[0]).map(mapfunc).map(givebatid)
	batsmenid = spark.read.text(sys.argv[3]).rdd.map(lambda r: r[0]).map(mapfunc).map(givebowlid)
	batvp = pvp.join(batsmenid).map(addbatid)
	replaced = bowlerid.join(batvp).map(addbowlid).map(toCSVLine)
	#batsmenid.saveAsTextFile('hdfs://localhost:9000/spark/out50.csv')
	#bowlerid.saveAsTextFile('hdfs://localhost:9000/spark/out51.csv')
	#clustercounts = replaced.map(add1)
	#clusterprob = clustercounts.reduceByKey(reducefunc).mapValues(findprob).map(toCSVLine)
	replaced.saveAsTextFile('hdfs://localhost:9000/spark/replaced1.csv')
	#bowlerid.saveAsTextFile('hdfs://localhost:9000/spark/bowlerids.csv')
	#batsmenid.saveAsTextFile('hdfs://localhost:9000/spark/batsmenids.csv')
	#Got ids from Final Batsmen and Final Bowlers






		

