#-*-coding:utf-8-*-
'''
可能存在多个药品id,药品名是一样的，对这样的费用进行合并
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
data=sc.textFile('file:///home/edu/mif/python/ss/Drug_Use_Payment_Model/feesComputedByyear310.txt')
data=data.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:((line[0],medicine[line[2]]),float(line[1])))\
    .reduceByKey(lambda a,b:a+b)\
    .collect()
out=open("feesComputed310.txt","w+")
for(key,value)in data:
    out.write("%s,%s,%.2f\n"%(key[0],key[1],value))
out.close()