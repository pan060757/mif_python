#-*-coding:utf-8-*-
'''
对每个病种统计每一年住院费用的支出情况,住院人次情况,得出每年各个病种费用支出排名，住院人次排名
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
####（（年份，区县代码,病种),(总费用，统筹费用支出,住院次数)）
result=hospital.map(precessing)\
    .map(lambda line:(('20'+line[21][-2:],line[0],line[27]),(float(line[6]),float(line[17]),1)))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
    .sortByKey()\

####（（年份，区县代码,病种),(总费用，统筹费用支出,住院次数,均次住院总费用,均次统筹费用支出)）
out=open('output/region_disease.csv','w+')
for (key,value)in result.collect():
    out.write("%s,%s,%s,%.2f,%.2f,%d,%.2f,%.2f\n"%(key[0],key[1],key[2],value[0],value[1],value[2],value[0]/value[2],value[1]/value[2]))
out.close()
