#-*-coding:utf-8 -*-
'''
检测门诊明细中是否存在重复收费的情况
'''
from pyspark import SparkContext
sc=SparkContext()




###门诊明细所有字段
detail = sc.textFile("/mif/data_new/worker_menzhen_detail.txt")
detail=detail.map(lambda line:line.encode('utf-8').split(','))\
    .filter(lambda line:len(line)==8)\
    .map(lambda line:((line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7]),1))\
    .reduceByKey(lambda a,b:a+b)\
    .filter(lambda(key,value):value>1)\
    .map(lambda(key,value):(key[0],(key[1],key[2],key[3],key[4],key[5],key[6],key[7],value)))\
    .sortByKey()

#####读入存在异常的门诊记录
# detail=sc.textFile("file:///home/edu/songsong/python/freItem/output/menzhen_dup.csv")
# detail=detail.map(lambda line:line.split(','))\
#     .map(lambda line:(line[0],(line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8])))\
#     .sortByKey()


#####读入门诊信息
#####（就医序号，（个人编号，医疗机构代码，门诊类型，总费用，结算时间））
menzhen = sc.textFile("/mif/data_new/worker_menzhen.txt")
menzhen=menzhen.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:(line[2],(line[1],line[3],line[4],line[5],line[15])))\

result=detail.join(menzhen)\
    .sortByKey()

###（(就诊费用记录,次数）
out = open('output/menzhen_duplicate.csv', 'w+')
for (key,value) in result.collect():
    out.write("%s,%s,%s,%s,%s,%s,%s,%s,%d%s,%s,%s,%s,%s\n" % (key,value[0][0],value[0][1],value[0][2],value[0][3],value[0][4],value[0][5],
                                                value[0][6],value[0][7],value[1][0],value[1][1],value[1][2],value[1][3],value[1][4]))
out.close()
