#-*-coding:utf-8-*-
'''
分别统计各个医院等级，在职和离退员工均次住院费用和均次统筹支付情况
'''
from pyspark import SparkContext
import sys
import string
##数据预处理
def precessing(line):
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

###对于进行连接的数据进行处理，计算参保人员年龄信息
####（人员编号，(年份,医院等级，住院费用，统筹支付，次数））
####（（年份.医院等级，在职离退状态)，(住院费用，统筹支付，次数)
def ageComputed((key,value)):
    try:
        current_year = value[0][0]
        birth_year = value[1][0][0:4]
        sex = value[1][1]
        age = string.atoi(current_year) - string.atoi(birth_year)  ###计算在职员工当前缴费时的年龄
        if (sex == '1'):
            if (age < 60):
                return ((current_year,value[0][1],'0'),(value[0][2],value[0][3],value[0][4]))
            else:
                return ((current_year,value[0][1],'1'),(value[0][2],value[0][3],value[0][4]))
        else:
            if (age < 50):
                return ((current_year,value[0][1],'0'),(value[0][2],value[0][3],value[0][4]))
            else:
                return ((current_year,value[0][1],'1'),(value[0][2],value[0][3],value[0][4]))
    except Exception:
        return (str(999999), 0)


##加入起付线、自付比例等因素
def postprocessing((key,value)):
    try:
        if(key[1]=='0'and key[2]=='0'):
            return (key, (value[0], value[1], 500,0.1))  ##无等级在职
        elif (key[1]=='0'and key[2]=='1'):
            return (key, (value[0], value[1], 400, 0.04))   ##无等级退休
        elif (key[1]=='1'and key[2]=='0'):
            return (key, (value[0], value[1], 500,0.1))  ##一级在职
        elif (key[1]=='1'and key[2]=='1'):
            return (key, (value[0], value[1], 400, 0.04))  ##一级退休
        elif(key[1]=='2'and key[2]=='0'):
            return (key, (value[0], value[1], 600, 0.15)) ##二级在职
        elif (key[1]=='2'and key[2]=='1'):
            return (key, (value[0], value[1], 500, 0.08)) ##二级在职
        elif (key[1]=='3'and key[2]=='0'):
            return (key, (value[0], value[1], 700, 0.2)) ##三级在职
        elif (key[1]=='3'and key[2]=='1'):
            return (key, (value[0], value[1], 600, 0.12)) ##三级退休
        elif (key[1]=='4'and key[2]=='0'):
            return (key, (value[0], value[1], 500,0.1))   ##社区在职
        elif (key[1]=='4'and key[2]=='1'):
            return (key, (value[0], value[1], 400,0.04))  ##社区退休
    except Exception:
        return (str(999999),0)



sc=SparkContext()
data=sc.textFile('/mif/data_new/worker_hospital.txt')
####（（人员编号,年份)(医院等级，住院费用，统筹支付，次数)）
####（（人员编号)(年份,医院等级，住院费用，统筹支付，次数)）
data1=data.map(precessing)\
    .map(lambda line:((line[1],'20'+line[21][-2:]),(line[5],float(line[6]),float(line[17]),1)))\
    .map(lambda (key,value):(key[0],(key[1],value[0],value[1],value[2],value[3])))\
    .sortByKey()

####(number,(birthdate,sex))
data2=sc.textFile('/mif/data_new/worker.txt')
data2=data2.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:(line[1],line[4:6]))


####（（年份.医院等级，在职离退状态)，(住院费用，统筹支付，次数))
####（（年份.医院等级，在职离退状态)，(均次住院费用，均次统筹支付))
####（（年份.医院等级，在职离退状态)，(均次住院费用，均次统筹支付,起付线,报销比例))
result=data1.join(data2)\
     .map(ageComputed)\
    .filter(lambda(key,value):(isinstance(value,int)==False))\
    .filter(lambda (key,value):cmp(key[0],"2009")>0 and cmp(key[0],"2017")<0 and key[1]!='5')\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
    .map(lambda (key,value):(key,(value[0]/value[2],value[1]/value[2])))\
    .map(postprocessing)\
    .filter(lambda(key,value):(isinstance(value,int)==False))\
    .sortByKey()


####（（年份.医院等级，在职离退状态)，(均次住院费用，均次统筹支付,起付线，自付比例))
out=open('output/hospitalizationFeesBygroup.csv','w+')
for (key,value) in result.collect():
    out.write("%s,%s,%s,%.2f,%.2f,%d,%.2f\n"%(key[0],key[1],key[2],value[0],value[1],value[2],value[3]))
out.close()
