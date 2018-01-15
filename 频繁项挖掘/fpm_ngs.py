#-*-coding:utf-8-*-
from pyspark import SparkContext
sc = SparkContext()

from pyspark.mllib.fpm import FPGrowth

nums = set()
reader = open('/home/edu/songsong/python/freItem/output/numberOfNGS.csv')
for num in reader:
    nums.add(num.strip('\n'))

data = sc.textFile("/mif/data/worker_hospital_detail.txt")
data = data.map(lambda line: line.encode("utf-8").split(','))
# num 0 ,medical_name 2 ,count 4
data_ngs = data.filter(lambda line: line[0] in nums and len(line) > 4)
#basket
data_bkt_withNum = data_ngs.map(lambda line: ((line[0], line[2]), 1)) \
    .reduceByKey(lambda a, b: a) \
    .map(lambda (k, v): (k[0], k[1])) \
    .reduceByKey(lambda a, b: a +'\t'+b)

####(人员编号，用过的药或者诊疗手段)
out=open('output/fpm_ngs.csv','w+')
for (key,value)in data_bkt_withNum.collect():
    out.write("%s,%s\n"%(key,value))
out.close()


