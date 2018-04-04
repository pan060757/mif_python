#-*-coding:utf-8 -*-
'''
涉及重复收费的门诊记录
'''
from pyspark import SparkContext
sc=SparkContext()
#####读入存在异常的门诊记录
data=sc.textFile("file:///home/edu/songsong/python/freItem/output/menzhen_duplicate.csv")
data=data.map(lambda line:line.split(','))\
     .map(lambda line:(line[0],(float(line[11]),1)))\
     .reduceByKey(lambda a,b:(a[0],a[1]+b[1]))\
     .sortByKey()

out = open('output/menzhen_dup_person.csv', 'w+')
for (key,value) in data.collect():
  out.write("%s,%.2f,%d\n"%(key,value[0],value[1]))
out.close()
