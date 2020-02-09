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

def mapcentroids(x):
	s = str(x[0])+","+str(x[1])
	global i 
	i += 1
	return s,i

def mapfunc(x):
	l=x.split(',')
	return l[1],l[2]

def	finddist(x):
	l1 = x[0].split(',')
	name = l1[0]
	a1 = float(l1[1])
	b1 = float(l1[2])
	a2 = float(x[1][0])
	b2 = float(x[1][1])
	dist = sqrt((a1-a2)**2 + (b1-b2)**2)
	return (name,(a2,b2,dist,a1,b1))

def findmindist(x,y):
	if(x[2]<y[2]):
		return x
	else:
		return y

def rearrange(x):
	a = x[1][0]
	b = x[1][1]
	dist = x[1][2]
	a1 = x[1][3]
	b1 = x[1][4]
	key = str(a)+","+str(b)
	value = (a1,b1,1)
	return key,value

def findnewcentroid(x,y):
	a = (x[0] + y[0])
	b = (x[1] + y[1])
	c = (x[2] + y[2])
	return a,b,c

def calc(x):
	a = x[1][0]
	b = x[1][1]
	c = x[1][2]
	d = round(a/c,4)
	e = round(b/c,4)
	return (d,e)

def check(ic,nc):
	for i in range(len(ic)):
		if ic[i] not in nc:
			return False
	return True

def mappoints(x):
	s = str(x[1][0])+","+str(x[1][1])
	return s,x[0]

def mapclusters(x):
	return x[1][0],x[1][1]

if __name__ == "__main__":
	
	spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
	initcentroids = lines.map(mapfunc).takeSample(False, 7, seed=0)
	initcentroids = sc.parallelize(initcentroids)
	combinations = lines.cartesian(initcentroids)
	dist = combinations.map(finddist).reduceByKey(findmindist)
	newcentroids = dist.map(rearrange).reduceByKey(findnewcentroid).map(calc)
	ic = initcentroids.collect()
	nc = newcentroids.collect()
	result = check(ic,nc)
	if result == True:
		newcentroids.saveAsTextFile('hdfs://localhost:9000/spark/bowlcentroids.csv')
	else:
		while True:
			oldcentroids = newcentroids
			combinations = lines.cartesian(oldcentroids)
			dist = combinations.map(finddist).reduceByKey(findmindist)
			newcentroids = dist.map(rearrange).reduceByKey(findnewcentroid).map(calc)
			oc = oldcentroids.collect()
			nc = newcentroids.collect()
			result = check(oc,nc)
			if result == True:
				break
		#newcentroids.saveAsTextFile('hdfs://localhost:9000/spark/out18.csv')
	centroids = newcentroids.map(mapcentroids)
	#centroids.saveAsTextFile('hdfs://localhost:9000/spark/out19.csv')
	points = dist.map(mappoints)
	#points.saveAsTextFile('hdfs://localhost:9000/spark/batpoints.csv')
	clusters = points.join(centroids).map(mapclusters)
	#.map(lambda x:(x[0],x[1]))
	clusters.saveAsTextFile('hdfs://localhost:9000/spark/bowlclusters.csv')



		

