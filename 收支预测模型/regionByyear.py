#-*-coding:utf-8-*-
'''
对每个区县每一年住院费用的支出情况,住院人次情况,得出每年各个区县费用支出排名，住院人次排名
'''
from pyspark import SparkContext
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

def h_precessing(line):
    line=line.encode('utf-8').split(',')
    for i in range(6,20):
        if(line[i]==""):
            line[i]='0'
    return line

def m_precessing(line):
    line=line.encode('utf-8').split(',')
    for i in range(5,13):
        if(line[i]==""):
            line[i]='0'
    return line

def leftProcessing((key,value)):
    if(value[1]==None):
        return (key,(value[0][0],value[0][1],value[0][2],0,0,0))
    else:
        return (key,(value[0][0],value[0][1],value[0][2],value[1][0],value[1][1],value[1][2]))

sc=SparkContext()
hospital=sc.textFile('/mif/data_new/worker_hospital.txt')
####（（年份，区县代码),(住院总费用，住院统筹费用支出,住院次数)）
hospital=hospital.map(h_precessing)\
    .map(lambda line:(('20'+line[21][-2:],line[0]),(float(line[6]),float(line[17]),1)))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
    .sortByKey()\

menzhen=sc.textFile('/mif/data_new/worker_menzhen.txt')
####（（年份，区县代码),(门诊总费用，门诊统筹费用支出,门诊次数)）
menzhen=menzhen.map(m_precessing)\
    .map(lambda line:((line[15][0:4],line[0]),(float(line[5]),float(line[12]),1)))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
    .sortByKey()\

####（（年份，区县代码),(住院总费用，住院统筹费用支出,住院次数,门诊总费用，门诊统筹费用支出,门诊次数)）
result=hospital.leftOuterJoin(menzhen)\
    .map(leftProcessing)\
    .sortByKey()

###（（年份，区县代码),(住院总费用，住院统筹费用支出,住院次数,门诊总费用，门诊统筹费用支出,门诊次数)）
out=open('output/regionByyear.csv','w+')
for (key,value)in result.collect():
    if(value[5]==0):
        out.write("%s,%s,%.2f,%.2f,%d,%.2f,%.2f,%.2f,%.2f,%d,%.2f,%.2f\n" % (
            key[0], key[1], value[0], value[1], value[2], value[0]/value[2], value[1]/value[2],
            value[3], value[4], value[5], 0, 0))
    else:
        out.write("%s,%s,%.2f,%.2f,%d,%.2f,%.2f,%.2f,%.2f,%d,%.2f,%.2f\n" % (
            key[0], key[1], value[0], value[1], value[2], value[0] / value[2], value[1] / value[2],
            value[3], value[4], value[5], value[3]/value[5],value[4]/value[5]))
out.close()
