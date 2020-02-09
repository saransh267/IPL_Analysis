import sys
import csv
from math import sqrt
import random
import bisect
import time

pvp = dict()
batids = dict()
bowlids = dict()
collabprob = dict()
wicketprob = dict()
with open('/home/kishan/Desktop/Project/Newstuff/CSV/bowlids.csv','rt')as a:
	data = csv.reader(a)
	for row in data:
		name = row[0].strip("'")
		c = row[1]
		bowlids[name] = c
a.close()
with open('/home/kishan/Desktop/Project/Newstuff/CSV/batids.csv','rt')as b:
	data = csv.reader(b)
	for row in data:
		name = row[0].strip("'")
		c = row[1]
		batids[name] = c
b.close()
with open('/home/kishan/Desktop/Project/Newstuff/CSV/pvp.csv','rt')as c:
	data = csv.reader(c)
	for row in data:
		bat = row[0].strip()
		bowl = row[1].strip()
		prob = list(map(float,row[2:]))
		for i in range(1,7):
			prob[i] = prob[i-1]+prob[i]
		if bat not in list(pvp.keys()):
			pvp[bat] = dict()
		pvp[bat][bowl] = prob
c.close()
with open('/home/kishan/Desktop/Project/Newstuff/CSV/collabprob.csv','rt')as d:
	data = csv.reader(d)
	for row in data:
		bid = row[0]
		bowlid = row[1]
		prob = list(map(float,row[2:]))
		for i in range(1,7):
			prob[i] = prob[i-1]+prob[i]
		if bid not in list(collabprob.keys()):
			collabprob[bid] = dict()
		collabprob[bid][bowlid] = prob
d.close()
#print(batids)
#print(bowlids)
#print(pvp)
#print(collabprob)
def predictruns(batsman,bowler):
	if (batsman not in list(pvp.keys())):
		batid = batids[batsman].strip()
		bowlid = bowlids[bowler].strip()
		prob = collabprob[batid][bowlid][:7]
		rand = random.random()
		runs = bisect.bisect(prob,rand)
		return runs
	else:	
		if (bowler not in list(pvp[batsman].keys())):
			batid = batids[batsman].strip()
			bowlid = bowlids[bowler].strip()
			prob = collabprob[batid][bowlid][:7]
			rand = random.random()
			runs = bisect.bisect(prob,rand)
			return runs
		else:
			prob = pvp[batsman][bowler][:7]
			balls = int(pvp[batsman][bowler][-1])
			if balls>6: 
				rand = random.random()
				runs = bisect.bisect(prob,rand)
				return runs
			else:
				batid = batids[batsman].strip()
				bowlid = bowlids[bowler].strip()
				prob = collabprob[batid][bowlid][:7]
				rand = random.random()
				runs = bisect.bisect(prob,rand)
				return runs

def predictwicket(batsman):
	wp = wicketprob[batsman]
	if wp < 0.5:
		return 1
	else:
		return 0 

def updatewicketprob(batsman,bowler):
	if batsman not in list(wicketprob.keys()):
		if (batsman not in list(pvp.keys())):
			batid = batids[batsman].strip()
			bowlid = bowlids[bowler].strip()
			wp = 1-collabprob[batid][bowlid][-1]
			wicketprob[batsman] = wp
		else:
			if (bowler not in list(pvp[batsman].keys())):
				batid = batids[batsman].strip()
				bowlid = bowlids[bowler].strip()
				wp = 1-collabprob[batid][bowlid][-1]
				wicketprob[batsman] = wp
			else:
				balls = int(pvp[batsman][bowler][-1])
				if balls>4: 
					wicketprob[batsman] = 1-pvp[batsman][bowler][-2]
				else:
					batid = batids[batsman].strip()
					bowlid = bowlids[bowler].strip()
					wp = 1-collabprob[batid][bowlid][-1]
					wicketprob[batsman] = wp
	else:
		if (batsman not in list(pvp.keys())):
			batid = batids[batsman].strip()
			bowlid = bowlids[bowler].strip()
			wp = 1-collabprob[batid][bowlid][-1]
			wicketprob[batsman] = wicketprob[batsman] * wp
		else:
			if (bowler not in list(pvp[batsman].keys())):
				batid = batids[batsman].strip()
				bowlid = bowlids[bowler].strip()
				wp = 1-collabprob[batid][bowlid][-1]
				wicketprob[batsman] = wicketprob[batsman] * wp
			else:
				balls = int(pvp[batsman][bowler][-1])
				if balls>4: 
					wp = 1-pvp[batsman][bowler][-2]
					wicketprob[batsman] = wicketprob[batsman] * wp
				else:
					batid = batids[batsman].strip()
					bowlid = bowlids[bowler].strip()
					wp = 1-collabprob[batid][bowlid][-1]
					wicketprob[batsman] = wicketprob[batsman] * wp

def printscore(runs,bon,bowler,balls,tr,w):
	over = balls//6
	ball = balls%6
	b = str(over)+'.'+str(ball)
	if runs == -1:
		print(b,"\tWICKET - ",bon,' bowled by ',bowler)
	else:
		print(b,"\t",str(tr)+'/'+str(w)+'\t',bon,' scored ',runs,' bowled by ',bowler)

team1batorder = ['DA Warner','S Dhawan','GH Vihari','MC Henriques','NV Ojha','RS Bopara','A Ashish Reddy','KV Sharma','P Kumar','B Kumar','TA Boult']
team2bowlorder = ['Sandeep Sharma','MG Johnson','Sandeep Sharma','MG Johnson','Sandeep Sharma','Anureet Singh','M Vijay','AR Patel','M Vijay','AR Patel','M Vijay','Anureet Singh','R Dhawan','MG Johnson','AR Patel','Anureet Singh','AR Patel','MG Johnson','Anureet Singh','Sandeep Sharma']
team2batorder = ['M Vijay','M Vohra','SE Marsh','GJ Bailey','DA Miller','WP Saha','AR Patel','R Dhawan','MG Johnson','Anureet Singh','Sandeep Sharma']
team1bowlorder = ['TA Boult','B Kumar','TA Boult','B Kumar','TA Boult','P Kumar','KV Sharma','MC Henriques','KV Sharma','MC Henriques','KV Sharma','MC Henriques','KV Sharma','P Kumar','MC Henriques','B Kumar','P Kumar','TA Boult','B Kumar','P Kumar']
def innings1(batorder,bowlorder):
	wickets = 0
	balls = 0
	bon = batorder[0]
	bnon = batorder[1]
	bowler = bowlorder[0]
	i=2
	j=1
	totalruns=0
	while(wickets<10 and balls<120):
		time.sleep(0.1)
		#print(i,wickets)
		balls += 1
		updatewicketprob(bon,bowler)
		if predictwicket(bon):
			wickets+=1
			printscore(-1,bon,bowler,balls,totalruns,wickets)
			if wickets == 10:
				continue
			i+=1
			bon = batorder[i-1]
		else:
			runs=predictruns(bon,bowler)
			totalruns += runs
			printscore(runs,bon,bowler,balls,totalruns,wickets)
			if(runs%2 == 1):
				bon,bnon = bnon,bon
		if(balls%6==0):
			bon,bnon = bnon,bon
			j+=1
			if balls == 120:
				continue
			bowler = bowlorder[j-1]
	return totalruns,wickets,balls

def innings2(batorder,bowlorder,target):
	wickets = 0
	balls = 0
	bon = batorder[0]
	bnon = batorder[1]
	bowler = bowlorder[0]
	i=2
	j=1
	totalruns=0
	while(wickets<10 and balls<120 and totalruns<=target):
		time.sleep(0.1)
		balls += 1
		updatewicketprob(bon,bowler)
		if predictwicket(bon):
			wickets+=1
			printscore(-1,bon,bowler,balls,totalruns,wickets)
			if wickets == 10:
				continue
			i+=1
			bon = batorder[i-1]
		else:
			runs=predictruns(bon,bowler)
			totalruns += runs
			printscore(runs,bon,bowler,balls,totalruns,wickets)
			if(runs%2 == 1):
				bon,bnon = bnon,bon
		if(balls%6==0):
			bon,bnon = bnon,bon
			j+=1
			if balls == 120:
				continue	
			bowler = bowlorder[j-1]
	return totalruns,wickets,balls

in1runs,in1wickets,in1balls = innings1(team1batorder,team2bowlorder)
print("End of innings1, score = ",str(in1runs)+'/'+str(in1wickets)," in ",str(in1balls//6)+'.'+str(in1balls%6)+' overs')
in2runs,in2wickets,in2balls = innings2(team2batorder,team1bowlorder,in1runs)
print("End of innings2, score = ",str(in2runs)+'/'+str(in2wickets)," in ",str(in2balls//6)+'.'+str(in2balls%6)+' overs')
if(in2runs > in1runs):
	print("Team 2 wins!")
else:
	print('Team 1 wins!')



