#-*-coding:utf-8 -*-
'''
参保人员各个年龄段所占比例
'''
from pyspark import SparkContext
import string

###程序入口
sc=SparkContext()
data=sc.textFile('file:///home/edu/songsong/python/ageModel/output/ageRatioDistribution.csv')
data=data.map(lambda line:line.encode("utf-8").split(","))\
    .map(lambda line:(line[0],(tuple(line[1:]))))\
    .reduceByKey(lambda a,b:a+b)\
    .sortByKey()


####(年份,年龄段，缴费人数)
out=open('output/ageRatioDistribution_1.csv','w+')
for (key,value) in data.collect():
    line = reduce(lambda a, b: "%s,%s" % (a, b), value).encode("utf-8")
    out.write("%s,%s\n" % (key, line))
out.close()