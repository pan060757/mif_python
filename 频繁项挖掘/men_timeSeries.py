#-*-coding:utf-8-*-
'''
就诊(门诊)时间序列挖掘
'''
import datetime
from pyspark import SparkContext
import sys
import re

###按照就诊时间顺序进行排序
def sortByDate(value1,value2):
    if(value1!=None and value2!=None):
        if (cmp(value1,value2)<0):
            return value1+','+value2
        else:
            return value2+','+value1
    elif value1!=None and value2==None:
        return value1
    elif value1==None and value2!=None:
        return value2
    else:
        return 999999


####读入职工住院数据
###（(个人编号,门诊结算时间）
###按照就诊时间顺序进行排序
sc=SparkContext()
data = sc.textFile("/mif/data_new/worker_menzhen.txt")
data=data.map(lambda line:line.encode("utf-8").split(",")) \
    .map(lambda line:((line[1],line[15]),1))\
    .sortByKey()\
    .map(lambda (key,value):(key[0],key[1]))\
    .reduceByKey(lambda a,b:a+','+b)\
    .sortByKey()

###（(个人编号,门诊结算时间）
out = open('output/menzhen_timeSeries.csv', 'w+')
for (key,value) in data.collect():
  out.write("%s,%s\n"%(key,value))
out.close()