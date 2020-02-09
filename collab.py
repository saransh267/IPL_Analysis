from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
#from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
import re
import sys
from operator import add
from pyspark.sql import SparkSession
from pyspark import SparkContext

'''
global i
i=0
def mapfunc(x):
	global i
	i+=1
	#for j in range(1,len(x)):
	#	x[j]=int(float((x[j])))
	x.append(i)
	return x
'''
if __name__ == "__main__":
	
	spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()
sc = SparkContext.getOrCreate()
#data = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
#test = spark.read.text(sys.argv[2]).rdd.map(lambda r: r[0])
data = sc.textFile("/home/kishan/Desktop/Project/Newstuff/datacolab.csv")
test = sc.textFile("/home/kishan/Desktop/Project/Newstuff/test.csv")
ratings = data.map(lambda l: l.split(','))\
    .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[9])))
testing = test.map(lambda l: l.split(','))\
    .map(lambda l: Rating(int(l[0]), int(l[1]),0.0))
rank = 10
numIterations = 10
model = ALS.train(ratings, rank, numIterations)
testdata = testing.map(lambda p: (p[0], p[1]))
predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2])).sortByKey()
#ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
#MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
#model.save(sc, "/home/kishan/Desktop/Project/Newstuff")
#sameModel = MatrixFactorizationModel.load(sc, "/home/kishan/Desktop/Project/Newstuff")
#userRecs = model.recommendForAllUsers(10)
predictions.saveAsTextFile('hdfs://localhost:9000/spark/colwk.csv')
#test.saveAsTextFile('hdfs://localhost:9000/spark/out16.csv')

