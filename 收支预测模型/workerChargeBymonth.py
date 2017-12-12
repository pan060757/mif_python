#-*-coding:utf-8 -*-
'''
职工统筹账户收入情况和参保人数(按月进行统计)
'''
from pyspark import SparkContext
import string
def precessing(line):
    line=line.encode('utf-8').split(',')
    for i in range(5,12):
        if(line[i]==""):
            line[i]='0'
    return line

sc=SparkContext()
data=sc.textFile('/mif/data_new/mode_ac43_310.txt')
###((月份，个人编号),(划入统筹账户,划入个人账户，次数))
###((月份,(划入统筹账户,划入个人账户，人数))
data=data.map(precessing)\
    .filter(lambda line:line[3]=='310')\
    .map(lambda line:((line[2][0:6],line[0]),(float(line[10]),float(line[11]),1)))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
    .map(lambda (key,value):(key[0],(value[0],value[1],1)))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
    .sortByKey()

###((月份,(总收入，统筹账户收入,个人账户收入，人数))
out=open('output/workerChargeBymonth.csv','w+')
for (key,value) in data.collect():
   out.write("%s,%.2f,%.2f,%.2f,%d\n"%(key,value[0]+value[1],value[0],value[1],value[2]))
out.close()

