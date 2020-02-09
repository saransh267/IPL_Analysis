import sys
import csv
from math import sqrt
infile = sys.stdin
#next(infile)
count = 0
points=[]
with open('/home/kishan/Desktop/Project/Newstuff/batclusters.csv','rt')as f:
	data = csv.reader(f)
	for row in data:
		name = row[0].strip('(').strip("'")
		c = row[1].strip(')').strip()
		points.append([name,c])
f.close()
with open('/home/kishan/Desktop/Project/Newstuff/batclusters.csv','w') as wr:
	writer = csv.writer(wr)
	for data in points:
		writer.writerow(data)
wr.close()