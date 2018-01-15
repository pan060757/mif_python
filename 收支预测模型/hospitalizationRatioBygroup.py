#-*-coding:utf-8-*-
'''
分别统计各个医院等级，在职和离退员工住院率计算（住院率=住院人次/参保总人数）
'''
from pyspark import SparkContext
import sys
import string

###参保数据预处理
###(（缴费年份，个人编号），缴费次数）
def c_preprocessing(line):
    try:
         line = line.encode("utf-8").split(",")
         if line[3]=="310":
            return ((line[2][0:4],line[0]),1)
         else:
             return(str(999999),0)
    except Exception:
         return (str(999999), 0)

##住院数据预处理
def h_precessing(line):
    line=line.encode('utf-8').split(',')
    if line[5]== "一级":
        line[5] = '1'
    elif line[5]== '无等级':
        line[5]= '0'
    elif line[5] == '二级':
        line[5]= '2'
    elif line[5]== '三级':
        line[5]= '3'
    elif line[5]== '社区':
        line[5]= '4'
    else:
        line[5]= '5'
    for i in range(6,20):
        if(line[i]==""):
            line[i]='0'
    return line

####参保人员年龄计算和状态划分
###(个人编号,(缴费年份,年龄，性别,状态))
def c_ageComputed((key,value)):
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


###对于进行连接的数据进行处理，计算住院人员年龄信息
####（人员编号，(年份,医院等级，住院次数））
####（（年份.医院等级，在职离退状态)，住院次数)
def h_ageComputed((key,value)):
    try:
        current_year = value[0][0]
        birth_year = value[1][0][0:4]
        sex = value[1][1]
        age = string.atoi(current_year) - string.atoi(birth_year)  ###计算在职员工当前缴费时的年龄
        if (sex == '1'):
            if (age < 60):
                return ((current_year,value[0][1],'0'),value[0][2])
            else:
                return ((current_year,value[0][1],'1'),value[0][2])
        else:
            if (age < 50):
                return ((current_year,value[0][1],'0'),value[0][2])
            else:
                return ((current_year,value[0][1],'1'),value[0][2])
    except Exception:
        return (str(999999), 0)

####((年份,状态)，次数)
def stateCount((key,value)):
    return ((value[0],value[3]),1)

sc=SparkContext()
data=sc.textFile('/mif/data_new/worker_hospital.txt')
####（（人员编号,年份)(医院等级，住院次数)）
####（（人员编号)(年份,医院等级，住院次数)）
data1=data.map(h_precessing)\
    .map(lambda line:((line[1],'20'+line[21][-2:]),(line[5],1)))\
    .map(lambda (key,value):(key[0],(key[1],value[0],value[1])))\
    .sortByKey()

####(number,(birthdate,sex))
data2=sc.textFile('/mif/data_new/worker.txt')
data2=data2.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:(line[1],line[4:6]))


####（（年份.医院等级，在职离退状态)，住院次数)
####((年份，在职离退状态),(医院等级，住院次数))
hospital=data1.join(data2)\
     .map(h_ageComputed)\
    .filter(lambda (key,value):cmp(key[0],"2005")>0 and cmp(key[0],"2017")<0 and key[1]!='5')\
    .reduceByKey(lambda a,b:a+b)\
    .map(lambda (key,value):((key[0],key[2]),(key[1],value)))\
    .sortByKey()

data3=sc.textFile('/mif/data/mode_ac43_310.txt')
###(（缴费年份，个人编号），缴费次数）
###(个人编号,(缴费年份,缴费次数）
data3=data3.map(c_preprocessing) \
    .filter(lambda (key,value):isinstance(key,str)==False) \
    .reduceByKey(lambda a, b: a + b)\
    .map(lambda (key,value):(key[1],(key[0],value)))

####((个人编号),(出生日期(yyyy-mm-dd)、性别))
data4=sc.textFile('/mif/data_new/worker.txt')
data4=data4.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:(line[1],line[4:6]))
#
# ###(个人编号,(缴费年份,缴费次数,出生日期，性别）
# ###(个人编号,(缴费年份,缴费次数,年龄，性别,状态）
# ####((年份，状态)，参保人数)
number=data3.join(data4)\
    .map(c_ageComputed)\
    .filter(lambda (key,value):(isinstance(value,int)==False))\
    .map(stateCount)\
    .reduceByKey(lambda a,b:a+b)\
    .sortByKey()\

###((年份，在职离退状态)，（医院等级，住院次数，参保人数）)
###((在职离退状态，医院等级)，（年份，住院次数，参保人数,住院率）)
result=hospital.join(number)\
    .sortByKey()\
    .map(lambda (key,value):((key[1],value[0][0]),(key[0],value[0][1],value[1],value[0][1]*1.0/value[1])))\
    .sortByKey()

###((在职离退状态，医院等级)，（年份，住院次数，参保人数,住院率）)
out=open('output/hospitalizationRatioBygroup.csv','w+')
for (key,value) in result.collect():
    line = reduce(lambda a, b: "%s,%s" % (a, b), value).encode("utf-8")
    out.write("%s,%s,%s\n" % (key[0],key[1],line))
out.close()
