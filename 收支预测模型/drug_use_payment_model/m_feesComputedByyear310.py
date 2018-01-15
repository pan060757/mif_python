#-*-coding:utf-8 -*-
'''
门诊：统计每年药品费用前10位的药品名
'''
from pyspark import SparkContext
import sys
####(（就医序号，药品id），总费用)
def getMedicine(line):
    try:
        line=line.encode('utf-8').split(',')
        if(len(line)>7 and line[7]!=""):
            if line[1] in med_List:
                return ((line[0], line[1]), float(line[7]))
            else:
                return (999999,1)
        else:
            return(999999,1)
    except Exception:
        return(999999,1)

######预处理worker_menzhen.txt
def preprocessing(line):
    try:
        line=line.encode('utf-8').split(',')
        return (line[2],line[15][0:4])
    except Exception:
        return (999999,0)

####程序入口
sc=SparkContext()
reload(sys)
sys.setdefaultencoding('utf-8')
medicine=sc.textFile('file:///home/edu/mif/python/ss/Drug_Use_Payment_Model/medicine.txt')
###得到药品目录的列表
medicine=medicine.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:(line[0],1))\
    .sortByKey()
med_List=medicine.keys().collect()

####读入职工门诊明细数据
data=sc.textFile('/mif/data_new/worker_menzhen_detail.txt')
####(（就医序号，药品id），总费用)
####(就医序号，（药品id，总费用）)
data1=data.map(getMedicine)\
    .filter(lambda (key,value):isinstance(key,int)==False)\
    .reduceByKey(lambda a,b:a+b)\
    .map(lambda(key,value):(key[0],(key[1],value)))\
    .sortByKey()

###(就医序号,入院日期）)
data=sc.textFile('/mif/data_new/worker_menzhen.txt')
data2=data.map(preprocessing) \
    .filter(lambda (key, value): isinstance(key, int) == False) \
    .sortByKey()

#####（就医序号,（药品id,总费用，入院日期））
#####(（入院日期,药品id），总费用)
#####(（入院日期，总费用)，药品id)  按照总费用进行排序
result=data1.join(data2)\
    .map(lambda (key,value):((value[1],value[0][0]),value[0][1]))\
    .reduceByKey(lambda a,b:a+b)\
    .map(lambda(key,value):((key[0],value),key[1]))\
    .sortByKey()

out=open('m_feesComputedByyear310.txt','w+')
for (key,value) in result.collect():
    out.write(key[0]+','+str(key[1])+','+value+'\r\n')
out.close()