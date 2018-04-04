#-*-coding:utf-8 -*-
'''
住院频次异常检测（在同一家医院月住院次数超过3次）
'''
from pyspark import SparkContext
import re
import datetime

####（（个人编号，月份，医疗机构代码，医院等级），次数）
def hospitalProcessing(line):
    line=line.encode('utf-8').split(',')
    if (line[21] != "" ):
        inHospital = line[21]
        outHospital = line[22]
        s = inHospital.strip("").split('-')
        t = outHospital.strip("").split('-')
        s[1] = re.sub("\D", "", s[1])  ##提取其中数字部分
        t[1] = re.sub("\D", "", t[1])  ##提取其中数字部分
        if len(s[1]) < 2:
            s[1] = '0' + s[1]
        if len(s[0]) < 2:
            s[0] = '0' + s[0]
        if len(t[1]) < 2:
            t[1] = '0' + t[1]
        if len(t[0]) < 2:
            t[0] = '0' + t[0]
        d1 = datetime.datetime(int('20' + s[2]), int(s[1]), int(s[0]))
        d2 = datetime.datetime(int('20' + t[2]), int(t[1]), int(t[0]))
        days = (d2 - d1).days
        inDate='20' + s[2]+s[1]
        return ((line[1],inDate,line[3],line[5]),(days,1))
    else:
        return (str(99999), 0)




sc=SparkContext()
data = sc.textFile("/mif/data_new/worker_hospital.txt")
####（个人编号，结算时间）
data=data.map(hospitalProcessing)\
    .filter(lambda (key,value):isinstance(key,str)==False)\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))\
    .filter(lambda (key,value):value[1]>3)\
    .sortByKey()


####记录在同一家医院月住院次数超过3次的数据记录
####（（个人编号，住院月份），次数）
out = open('output/hospital_frequency.csv', 'w+')
for (key, value) in data.collect():
    out.write("%s,%s,%s,%s,%d,%d\n" % (key[0],key[1],key[2],key[3],value[0],value[1]))
out.close()