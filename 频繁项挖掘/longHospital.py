#-*-coding:utf-8 -*-
'''
超长期住院
'''
from pyspark import SparkContext
sc=SparkContext()
data=sc.textFile("file:///home/edu/songsong/python/freItem/output/hospitalDays.csv")
data=data.map(lambda line:line.encode('utf-8').split(','))\
    .filter(lambda line:int(line[10])>30) \
    .map(lambda line:(line[0], (line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10]))) \
    .sortByKey() \

out = open('output/longHospital.csv', 'w+')
for (key, value) in data.collect():
    line = reduce(lambda a, b: "%s,%s" % (a, b),value)
    out.write("%s,%s\n" % (key, line))
out.close()

