# -*-coding:utf-8 -*-
'''
用于住院天数异常检测
'''

import datetime
from pyspark import SparkContext
import sys
import re
import string

#####住院数据预处理
###（(个人编号 ,(年份，医疗机构代码 3,住院类别 4，医院等级 5，出院病种名称 27，统筹费用 17，（住院天数）))
def hospitalProcessing(line):
    ### 医院等级的划分
    line = line.encode("utf-8").split(",")
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
    for i in range(6, 20):
        if line[i] == "":
            line[i] = '0'
        if float(line[i]) < 0:
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
        if days < 0:
            return (str(999999), 1)
        return (line[1], ('20'+line[21][-2:],line[3], line[4], line[5], line[27], line[17], days))
    else:
        return (str(999999), 1)

###对于进行连接的数据进行处理，计算参保人员年龄信息
###（(个人编号 ,(年份，年龄，性别，医疗机构代码 3,住院类别 4，医院等级 5，出院病种名称 27，统筹费用 17，住院天数）))
def ageComputed((key, value)):
    try:
        current_year = value[0][0]
        birth_year = value[1][0][0:4]
        age = string.atoi(current_year) - string.atoi(birth_year)  ###计算在职员工当前缴费时的年龄
        return (key, (current_year, age,value[1][1], value[0][1], value[0][2],value[0][3], value[0][4],value[0][5], value[0][6]))
    except Exception:
        return (str(999999), 0)


###（(个人编号 ,(年份，年龄，年龄组，性别，医疗机构代码 3,住院类别 4，医院等级 5，出院病种名称 27，统筹费用 17，（住院天数）))
def ageDivided((key, value)):
    year = value[0]
    age = value[1]
    age_group = '0'
    if age in range(0, 20):
        age_group = '0'
    elif age in range(20, 30):
        age_group = '1'
    elif age in range(30, 40):
        age_group = '2'
    elif age in range(40, 50):
        age_group = '3'
    elif age in range(50, 60):
        age_group = '4'
    elif age in range(60, 70):
        age_group = '5'
    elif age in range(70, 80):
        age_group = '6'
    elif age in range(80, 90):
        age_group = '7'
    elif age in range(90, 100):
        age_group = '8'
    elif age in range(100, 150):
        age_group = '9'
    else:
        age_group='0'
    return (key,(year, age,age_group,value[2], value[3], value[4],value[5], value[6],value[7], value[8]))


####读入职工住院数据

###按照就诊时间顺序进行排序
###（(个人编号 ,(年份，医疗机构代码 3,住院类别 4，医院等级 5，出院病种名称 27，统筹费用 17，（住院天数）))
sc = SparkContext()
data1 = sc.textFile("/mif/data_new/worker_hospital.txt")
data1 = data1.map(hospitalProcessing) \
    .filter(lambda (key, value): isinstance(value, int) == False) \
    .sortByKey()

    ####(number,(birthdate,sex))
data2 = sc.textFile('/mif/data_new/worker.txt')
data2 = data2.map(lambda line: line.encode('utf-8').split(',')) \
    .map(lambda line: (line[1], line[4:6]))

###（(个人编号 ,(年份，年龄，年龄组，性别，医疗机构代码 3,住院类别 4，医院等级 5，出院病种名称 27，统筹费用 17，住院天数））
result=data1.join(data2)\
    .map(ageComputed)\
    .filter(lambda(key,value):isinstance(value,int)==False)\
    .map(ageDivided) \
    .sortByKey()


###（(个人编号 ),(入院时间，出院时间)）
out = open('output/hospitalDays.csv', 'w+')
for (key, value) in result.collect():
    line = reduce(lambda a, b: "%s,%s" % (a, b), value)
    out.write("%s,%s\n" % (key,line))
out.close()
