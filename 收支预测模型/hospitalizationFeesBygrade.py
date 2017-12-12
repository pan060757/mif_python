#-*-coding:utf-8 -*-
'''
分年度比较，不同等级医院医疗总费用、医疗统筹账户支付增减情况(所以只对住院费用进行比较)
'''
from pyspark import SparkContext
import sys

sc=SparkContext()
reload(sys)
sys.setdefaultencoding('utf-8')

##数据预处理
def precessing(line):
    line=line.encode('utf-8').split(',')
    for i in range(6,20):
        if(line[i]==""):
            line[i]='0'
    return line

###住院费用
data=sc.textFile('/mif/data_new/worker_hospital.txt')
##((年份，医院等级)，（住院总费用，统筹账户支付,住院人次))
hospital=data.map(precessing)\
    .map(lambda line:(('20'+line[21][-2:],line[5]),(float(line[6]),float(line[17]),1)))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
    .sortByKey()

out=open('output/hospitalizationFeesBygrade.csv','w+')
####((年份，医院等级),(住院总费用，住院统筹账户支付，住院人次，均次住院费用，均次统筹账户支付))
for(key,value)in hospital.collect():
    out.write('%s,%s,%.2f,%.2f,%d,%.2f,%.2f\n'% (key[0],key[1],value[0],value[1],value[2],value[0]/value[2],value[1]/value[2]))
out.close()

