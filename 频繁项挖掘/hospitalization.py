#-*-coding:utf-8 -*-
'''
分解住院行为检测
'''
from pyspark import SparkContext

def computeOutliers(line):
    i=2
    count=0
    while(i<len(line)-1):
        if(line[i]==line[i+1]):
            count = count + 1
        i=i+2
    return (line[0],count)

sc=SparkContext()
data=sc.textFile("file:///home/edu/songsong/python/freItem/output/hos_timeSeries.csv")
data=data.map(lambda line:line.split(','))\
    .map(computeOutliers)\
    .filter(lambda (key,value):value>0)\
    .sortByKey()

out = open('output/hospitalization.csv', 'w+')
for (key, value) in data.collect():
    out.write("%s,%d\n" % (key,value))
out.close()