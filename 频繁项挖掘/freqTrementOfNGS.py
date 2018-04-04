###-*-coding:utf-8 -*-
'''
利用FPGrowth挖掘脑梗死病人的频繁用药模式（2015年度住院费用总支出总高，统筹费用支出最高）
'''
from pyspark import SparkContext
from pyspark.mllib.fpm import FPGrowth
import string
import sys
sc = SparkContext()
reload(sys)
sys.setdefaultencoding("utf-8")
nums = set()
reader = open('/home/edu/mif/python/ss/mif/drug_disease/output/numberOfNGS.csv')
for num in reader:
    nums.add(num.strip('\n'))
nums_broadcast=sc.broadcast(nums)

reader = open('/home/edu/mif/python/ss/mif/drug_disease/output/treatment.txt')
trement=set()
for num in reader:
    trement.add(num.strip("\n").split(',')[0])
trement_broadcast=sc.broadcast(trement)

# def removeDuplicate((key,value)):
#     values=value.split(',')
#     dup = values[0]
#     for v in values:
#         if v not in dup:
#             dup=dup+' '+v
#     return (key,dup)


data = sc.textFile("/mif/data_new/worker_hospital_detail.txt")
data = data.map(lambda line: line.split(','))
# num 0 ,medical_name 2 ,count 4
#####诊疗频繁模式挖掘
trement_nums = data.filter(lambda line: line[0] in nums_broadcast.value)
trement_ngs=trement_nums.filter(lambda line:line[1] in trement_broadcast.value)
trement_bkt_withNum = trement_ngs.map(lambda line: ((line[0], line[2]), 1)) \
    .reduceByKey(lambda a, b: a+b) \
    .map(lambda (key,value):(key[0],key[1]))\
    .reduceByKey(lambda a,b:a+' '+b)

####(人员编号，诊断集合)
out=open('output/freqTrementOfNGS.csv','w+')
for (key,value)in trement_bkt_withNum.collect():
    out.write("%s\n"%(value))
out.close()