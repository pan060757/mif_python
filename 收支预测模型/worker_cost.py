#-*-coding:utf-8 -*-
'''
统计每年的住院总费用，住院统筹费用支出，门诊总费用，门诊统筹支出
'''
from pyspark import SparkContext
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

def precessing(line):
    line=line.encode('utf-8').split(',')
    for i in range(6,20):
        if(line[i]==""):
            line[i]='0'
    return line

sc=SparkContext()
hospital=sc.textFile('/mif/data_new/worker_hospital.txt')
####（（年份，(总费用，统筹费用支出,次数)）
hospital=hospital.map(precessing)\
    .map(lambda line:('20'+line[21][-2:],(float(line[6]),float(line[17]),1)))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
    .sortByKey()

men_zhen=sc.textFile('/mif/data_new/worker_menzhen.txt')
####（（年份，(总费用，统筹费用支出，门诊人次)）
men_zhen=men_zhen.map(lambda line:line.encode('utf-8').split(','))\
    .filter(lambda line:line[5]!="" and line[12]!="")\
    .map(lambda line:(line[15][0:4],(float(line[5]),float(line[12]),1)))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
    .sortByKey()

###（年份，（住院总费用，住院统筹支付，门诊总费用，门诊统筹支出，住院人次，门诊人次））
result=hospital.join(men_zhen)\
    .map(lambda (key,value):(key,(value[0][0],value[0][1],value[1][0],value[1][1],value[0][2],value[1][2])))\
    .sortByKey()

###（年份，(总费用支出，统筹总支出，住院总费用，住院统筹支付，门诊总费用，门诊统筹支出，住院人次，门诊人次，均次住院统筹支付，均次门诊统筹支付））
out=open('output/worker_cost.csv','w+')
for (key,value)in result.collect():
    out.write("%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%d,%.2f,%.2f\n"%(key,value[0]+value[2],value[1]+value[3],value[0],value[1],value[2],value[3],value[4],value[5],value[1]/value[4],value[3]/value[5]))
out.close()

