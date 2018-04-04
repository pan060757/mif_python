#-*-coding:utf-8-*-
'''
就诊(住院)时间序列挖掘
'''
import datetime
from pyspark import SparkContext
import sys
import re

#####住院数据预处理
###（(个人编号,入院日期),出院日期))
def hospitalProcessing(line):
    ### 医院等级的划分
    line=line.encode("utf-8").split(",")
    ### 医院等级的划分
    if (line[5] == '无等级'):
        line[5] = '0'
    elif (line[5] == '一级'):
        line[5] = '1'
    elif (line[5] == '二级'):
        line[5] = '2'
    elif (line[5] == '三级'):
        line[5] = '3'
    elif (line[5] == '社区'):
        line[5] = '4'
    else:
        line[5] = '5'
    for i in range(6,20):
        if line[i]=="":
            line[i]='0'
        if float(line[i])<0:
            return (str(999999), 1)
    if (line[21] != "" and line[22] != ""):
        inHospital = line[21]  ###入院日期
        outHospital = line[22]  ###出院日期
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
        inHospital = '20' + s[2] + s[1] + s[0]
        outHospital = '20' + t[2] + t[1] + t[0]
        d1 = datetime.datetime(int('20' + s[2]), int(s[1]), int(s[0]))
        d2 = datetime.datetime(int('20' + t[2]), int(t[1]), int(t[0]))
        days = (d2 - d1).days
        if days<0:
            return (str(999999), 1)
        return ((line[1],inHospital),outHospital)
    else:
        return (str(999999), 1)

####读入职工住院数据
###（(个人编号,(入院日期，出院日期)）
###按照就诊时间顺序进行排序
sc=SparkContext()
data = sc.textFile("/mif/data_new/worker_hospital.txt")
data=data.map(hospitalProcessing) \
    .filter(lambda (key, value): isinstance(value,int) == False) \
    .sortByKey()\
    .map(lambda (key,value):(key[0],(key[1],value)))\
    .reduceByKey(lambda a,b:a+b)\
    .sortByKey()

###（(个人编号 ),(入院时间，出院时间)）
out = open('output/hos_timeSeries.csv', 'w+')
for (key,value) in data.collect():
  line= reduce(lambda a, b: "%s,%s"%(a,b),value).encode("utf-8")
  out.write("%s,%s\n"%(key,line))
out.close()