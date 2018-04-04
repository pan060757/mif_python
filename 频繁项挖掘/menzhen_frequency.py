#-*-coding:utf-8 -*-
'''
门诊频次异常检测（月门诊次数超过15次）
'''
from pyspark import SparkContext
sc=SparkContext()

data = sc.textFile("/mif/data_new/worker_menzhen.txt")

####（个人编号，结算时间）
data=data.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:((line[1],line[15][0:6]),1))\
    .reduceByKey(lambda a,b:a+b)\
    .filter(lambda (key,value):value>15)\
    .sortByKey()


####记录月门诊人次超过15次的数据记录
####（（个人编号，门诊月份），次数）
out = open('output/menzhen_frequency.csv', 'w+')
for (key, value) in data.collect():
    out.write("%s,%s,%d\n" % (key[0],key[1],value))
out.close()