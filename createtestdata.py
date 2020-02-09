import sys
import csv
points=[]
for i in range(1,561):
	for j in range(1,561):
		if i == j:
			continue
		points.append([i,j])
with open('/home/kishan/Desktop/Project/Newstuff/test.csv','w') as wr:
	writer = csv.writer(wr)
	for data in points:
		writer.writerow(data)
wr.close()