#-*-coding:utf-8-*-
'''
将城镇职工医疗保险的数据和城镇居民医疗保险的数据进行汇总，得出费用前10的药品
'''
from pyspark import SparkContext


sc=SparkContext()
####读入药品数据
data1=open('medicine.txt')
medicine={}
for line in data1:
    line=line.split(',')
    medicine[line[0]]=line[1]
broadAvgWage=sc.broadcast(medicine)        ###广播该变量

###((年份，药品名),总费用)
###((年份，总费用)，药品名)
data=sc.textFile('file:///home/edu/mif/python/ss/Drug_Use_Payment_Model/feesComputedByyear.txt')
data=data.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:((line[0],medicine[line[2]]),float(line[1])))\
    .reduceByKey(lambda a,b:a+b)\
    .map(lambda (key,value):((key[0],value),key[1]))\
    .sortByKey()\
    .collect()
out=open("feesComputed.txt","w+")
for(key,value)in data:
    out.write("%s,%.2f,%s\n"%(key[0],key[1],value))
out.close()