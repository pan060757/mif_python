#-*-coding:utf-8 -*-
'''
对参保人员的在职离退状态进行划分
'''
from pyspark import SparkContext
import string

###(（缴费年份，个人编号），缴费次数）
def preprocessing(line):
    try:
         line = line.encode("utf-8").split(",")
         if line[3]=="310":
            return ((line[2][0:4],line[0]),1)
         else:
             return(str(999999),0)
    except Exception:
         return (str(999999), 0)

####年龄计算和状态划分
###(个人编号,(缴费年份,年龄，性别,状态))
def ageComputed((key,value)):
    try:
        current_year=value[0][0]
        birth_year=value[1][0][0:4]
        sex=value[1][1]
        age=string.atoi(current_year)-string.atoi(birth_year)         ###计算在职员工当前缴费时的年龄
        if (sex == '1'):
            if (age<60):
                return (key,(value[0][0],age,value[1][1],'0'))
            else:
                return (key,(value[0][0],age,value[1][1],'1'))
        else:
            if (age < 50):
                return (key,(value[0][0],age,value[1][1],'0'))
            else:
                return (key,(value[0][0],age,value[1][1],'1'))
    except Exception:
        return (str(999999),0)


####((年份,状态)，次数)
def stateCount((key,value)):
    return ((value[0],value[3]),1)

sc=SparkContext()
data=sc.textFile('/mif/data/mode_ac43_310.txt')
###(（缴费年份，个人编号），缴费次数）
###(个人编号,(缴费年份,缴费次数）
data1=data.map(preprocessing) \
    .filter(lambda (key,value):isinstance(key,str)==False) \
    .reduceByKey(lambda a, b: a + b)\
    .map(lambda (key,value):(key[1],(key[0],value)))

####((个人编号),(出生日期(yyyy-mm-dd)、性别))
data2=sc.textFile('/mif/data_new/worker.txt')
data2=data2.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:(line[1],line[4:6]))
#
# ###(个人编号,(缴费年份,缴费次数,出生日期，性别）
# ###(个人编号,(缴费年份,缴费次数,年龄，性别,状态）
# ####((年份，状态)，参保人数)
result=data1.join(data2)\
    .map(ageComputed)\
    .filter(lambda (key,value):(isinstance(value,int)==False))\
    .map(stateCount)\
    .reduceByKey(lambda a,b:a+b)\
    .sortByKey()\
    .map(lambda (key,value):(key[0],(key[1],value)))\
    .reduceByKey(lambda a,b:a+b)\
    .filter(lambda (key,value):(len(value)==4))\
    .sortByKey()

out=open("output/working_retired.csv","w+")
for (key,value) in result.collect():
    out.write("%s,%d,%s,%d,%s,%d\n"%(key,value[1]+value[3],value[0],value[1],value[2],value[3]))
out.close()