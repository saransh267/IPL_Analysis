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

def mapfunc(x):
	l=x.split(',')
	return (l[0],l[1:])

def mapbatvp(x):
	l=x[1][0]
	m=x[1][1]
	n=l[:1]+m+l[1:]
	return (n[0],n[1:])

def mapclusters(x):
	bowlcluster = x[1][0][0].strip()
	batcluster = x[1][1][0].strip()
	key = batcluster+":"+bowlcluster
	return key,x[1][1][1:]

def add1(x):
	key = x[0]
	value = x[1]
	value.append(1)
	return key,value

def reducefunc(x,y):
	l=[]
	for i in range(len(x)):
		j=float(x[i])
		k=float(y[i])
		m=j+k
		l.append(m)
	return l

def findprob(x):
	li=[]
	for i in range(len(x)-2):
		num = round(float(x[i])/float(x[-1]),4)
		li.append(num)
	li.append(int(x[-2]))
	return li

def toCSVLine(data):
	key = data[0]
	l = data[1]
	s = ''
	k = 0
	for i in key.split(':'):
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

if __name__ == "__main__":
	
	spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

	pvp = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0]).map(mapfunc)
	bowlers = spark.read.text(sys.argv[2]).rdd.map(lambda r: r[0]).map(mapfunc)
	batsmen = spark.read.text(sys.argv[3]).rdd.map(lambda r: r[0]).map(mapfunc)
	batvp = pvp.join(batsmen).map(mapbatvp)
	replaced = bowlers.join(batvp).map(mapclusters)
	clustercounts = replaced.map(add1)
	clusterprob = clustercounts.reduceByKey(reducefunc).mapValues(findprob).map(toCSVLine)
	clusterprob.saveAsTextFile('hdfs://localhost:9000/spark/clusterprob1.csv')





		

