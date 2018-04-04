#-*-coding:utf-8 -*-
'''
统计各等级医院个数
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
####（（年份，医疗机构代码,医院等级),1)）
hospital=hospital.map(precessing)\
    .map(lambda line:(('20'+line[21][-2:],line[3],line[5]),1))\
    .reduceByKey(lambda a,b:a+b)\
    .map(lambda (key,value):(key[0],key[2]),1)\
    .reduceByKey(lambda a,b:a+b)\
    .sortByKey()



###（（年份，医院等级),个数)
out=open('output/hospital_grade.csv','w+')
for (key,value)in hospital.collect():
    out.write(str(key)+','+str(value)+"\n")
out.close()

