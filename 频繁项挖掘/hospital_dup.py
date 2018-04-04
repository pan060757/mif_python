#-*-coding:utf-8 -*-
'''
检测住院明细中是否存在重复收费的情况
'''
from pyspark import SparkContext
sc=SparkContext()
data = sc.textFile("/mif/data_new/worker_hospital_detail.txt")
data=data.map(lambda line:line.encode("utf-8").split(','))\
    .filter(lambda line:len(line)==8)\
    .map(lambda line:((line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7]),1))\
    .reduceByKey(lambda a,b:a+b)\
    .filter(lambda(key,value):value>1)\
    .sortByKey()


#####读入门诊信息
#####（就医序号，（个人编号，医疗机构代码，住院类别 ，医院等级，总费用，住院时间））
hospital = sc.textFile("/mif/data_new/worker_hospital.txt")
hospital=hospital.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:(line[2],(line[1],line[3],line[4],line[5],line[6],'20'+line[21][-2:])))\

result=data.join(hospital)\
    .sortByKey()



###（(就诊费用记录,次数）
out = open('output/hospital_duplicate.csv', 'w+')
for (key,value) in result.collect():
    out.write("%s,%s,%s,%s,%s,%s,%s,%s,%d%s,%s,%s,%s,%s,%s\n" % (
    key, value[0][0], value[0][1], value[0][2], value[0][3], value[0][4], value[0][5],
    value[0][6], value[0][7], value[1][0], value[1][1], value[1][2], value[1][3], value[1][4],value[1][5]))
out.close()
