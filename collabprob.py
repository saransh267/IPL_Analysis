from __future__ import print_function
import re
import sys
from operator import add
from pyspark.sql import SparkSession
from pyspark import SparkContext
from math import sqrt

sc =SparkContext()

def mapfunc(x):
	key = x[0]
	result=list(x[1][0])
	result.append(x[1][1])
	return key,result

def stripfunc(x):
	l = x.split(',')
	for i in range(len(l)):
		l[i] = l[i].strip().strip('(').strip(')')
	return ((l[0],l[1]),l[2])

def normalize(x):
	li = list(map(float,x[:7]))
	minval = abs(min(li))
	for i in range(len(li)):
		li[i] += minval 
	s = sum(li)
	abc=0
	try:
		for j in range(len(li)):
			li[j] /= s
	except:
		abc+=1
	wk = float(x[7])
	if wk<0:
		li.append(0)
	else:
		while(wk>0.5):
			wk = wk*0.7
		li.append(wk)
	return li

def toCSVLine(data):
	key = data[0]
	l = data[1]
	s = ''
	k = 0
	for i in key:
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

	prob0 = sc.textFile("/home/kishan/Desktop/Project/Newstuff/CSV/c0.csv").map(stripfunc)
	prob1 = sc.textFile("/home/kishan/Desktop/Project/Newstuff/CSV/c1.csv").map(stripfunc)
	prob2 = sc.textFile("/home/kishan/Desktop/Project/Newstuff/CSV/c2.csv").map(stripfunc)
	prob3 = sc.textFile("/home/kishan/Desktop/Project/Newstuff/CSV/c3.csv").map(stripfunc)
	prob4 = sc.textFile("/home/kishan/Desktop/Project/Newstuff/CSV/c4.csv").map(stripfunc)
	prob5 = sc.textFile("/home/kishan/Desktop/Project/Newstuff/CSV/c5.csv").map(stripfunc)
	prob6 = sc.textFile("/home/kishan/Desktop/Project/Newstuff/CSV/c6.csv").map(stripfunc)
	probwk = sc.textFile("/home/kishan/Desktop/Project/Newstuff/CSV/cwk.csv").map(stripfunc)
	final = prob0.join(prob1).join(prob2).map(mapfunc).join(prob3).map(mapfunc).join(prob4).map(mapfunc).join(prob5).map(mapfunc).join(prob6).map(mapfunc).join(probwk).map(mapfunc)
	normalized = final.mapValues(normalize).map(toCSVLine)
	normalized.saveAsTextFile('hdfs://localhost:9000/spark/probcollab2.csv')





		

