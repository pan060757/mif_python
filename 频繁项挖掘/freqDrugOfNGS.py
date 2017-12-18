###-*-coding:utf-8 -*-
'''
利用FPGrowth挖掘脑梗死病人的频繁用药模式（2015年度住院费用总支出总高，统筹费用支出最高）
'''
from pyspark import SparkContext
import sys
sc = SparkContext()
reload(sys)
sys.setdefaultencoding("utf-8")

from pyspark.mllib.fpm import FPGrowth

nums = set()
reader = open('/home/edu/mif/python/ss/mif/drug_disease/output/numberOfNGS.csv')
for num in reader:
    nums.add(num.strip('\n'))
nums_broadcast=sc.broadcast(nums)


reader = open('/home/edu/mif/python/ss/mif/drug_disease/output/medicine.txt')
medicine=set()
for num in reader:
    medicine.add(num.strip("\n").split(',')[0])
medicine_broadcast=sc.broadcast(medicine)

data = sc.textFile("/mif/data_new/worker_hospital_detail.txt")
data = data.map(lambda line: line.split(','))
# num 0 ,medical_name 2 ,count 4
#####用药频繁模式挖掘
drug_nums = data.filter(lambda line: line[0] in nums_broadcast.value)
drug_ngs=drug_nums.filter(lambda line:line[1] in medicine_broadcast.value)
drug_bkt_withNum = drug_ngs.map(lambda line: ((line[0], line[2]), 1)) \
    .reduceByKey(lambda a, b: a) \
    .map(lambda (k, v): (k[0], [k[1]])) \
    .reduceByKey(lambda a, b: a + b)
drug_bkt=drug_bkt_withNum.map(lambda (k, v): v)
drug_bkt.cache()
drug_model = FPGrowth.train(drug_bkt, 0.1)
drug_fitems = drug_model.freqItemsets().collect()
out = open('/home/edu/mif/python/ss/mif/drug_disease/output/drugOfNGS.txt', 'w+')
for itemset in drug_fitems:
    line = reduce(lambda a, b: "%s\t%s"%(a,b), itemset.items).encode("utf-8")
    out.write("%d\t%s\n" % (itemset.freq,line))
out.close()
