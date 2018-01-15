#-*-coding:utf-8-*-
'''
参保人员分类管理
'''
import datetime
from pyspark import SparkContext
import sys
import re

#####住院数据预处理
###（(个人编号 1,年份),(就医序号 2，医院等级 5，住院天数，个人支付 20)))）
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
    if(line[21]!="" and line[22]!=""):       ###可能存在未激励出院时间和住院时间的住院记录
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
        if days<0:
            return (str(999999), 1)
        return ((line[1],'20'+s[2]),(line[2],line[5],days,float(line[20])))
    else:
        return (str(999999), 1)

###(个人编号 0，年份 ，年度工资 5)存在重复
def chargeProcessing(line):
    line = line.encode("utf-8").split(",")
    if  line[3]=='310':
        if line[5]=="":
            line[5]='0'
        if float(line[5])<0:
            return (str(999999), 1)
        return((line[0],line[2][0:4]),float(line[5]))
    else:
        return(str(999999),1)

####去重
def removeDupl(value1,value2):
    return value1

####计算年龄
######(个人编号,(年份，工作性质，出生日期,性别,年度工资，累计住院天数,住院次数，累计个人支付 20))
######(个人编号,(年份，年龄，离退状态,工作性质,性别,年度工资，累计住院天数,住院次数，累计个人支付 20))
def ageComputed((key, value)):
    currentDate = value[0]  ###入院日期
    birthDate = value[2][0:4]  ###出生日期
    sex=value[3]
    age = int(currentDate) - int(birthDate)  ##计算住院时年龄
    if (sex == '1'):
        if (age < 60):
            return (key, (value[0], age, '0', value[1],value[3],value[4],value[5],value[6],value[7]))
        else:
            return (key, (value[0], age, '1', value[1],value[3],value[4],value[5],value[6],value[7]))
    else:
        if (age < 50):
            return (key, (value[0], age, '0', value[1],value[3],value[4],value[5],value[6],value[7]))
        else:
            return (key, (value[0], age, '1', value[1],value[3],value[4],value[5],value[6],value[7]))

####是否患有慢性病
def chroricProcessing((key,value)):
    if value[1]==None:
        return (key, (value[0][0], value[0][1], value[0][2],value[0][3],value[0][4],value[0][5],'0',value[0][6],
                      value[0][7], value[0][8]))
    else:
        return (key, (value[0][0], value[0][1], value[0][2], value[0][3], value[0][4], value[0][5],'1', value[0][6],
                      value[0][7], value[0][8]))


####((个人编号,医院等级),(住院人次))
####(个人编号,(医院等级，住院人次))
####读入职工住院数据
###（（个人编号,年份）,(就医序号，医院等级 5，住院天数,个人支付 20）
###（（个人编号,年份），(住院天数,住院次数，个人支付 20）
###（（个人编号,年份），(累计住院天数,住院次数，累计个人支付 20）
sc=SparkContext()
data = sc.textFile("/mif/data_new/worker_hospital.txt")
data=data.map(hospitalProcessing) \
    .filter(lambda (key, value): isinstance(value,int) == False) \
    .map(lambda (key,value):((key,(value[2],1,value[3]))))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
    .sortByKey()


#####读入职工缴费数据
######(个人编号，年度工资)
charge=sc.textFile("/mif/data_new/mode_ac43_310.txt")
charge=charge.map(chargeProcessing)\
    .filter(lambda (key,value):(isinstance(value,int)==False))\
    .reduceByKey(lambda a,b:a+b)\
    .filter(lambda (key,value):(value>0))\
    .sortByKey()

#####(（个人编号,年份),((年度工资，累计住院天数,住院次数，累计个人支付 20))
#####(（个人编号),((年份，年度工资，累计住院天数,住院次数，累计个人支付 20)
hospitalCharge=data.join(charge)\
    .map(lambda (key,value):(key[0],(key[1],value[1],value[0][0],value[0][1],value[0][2])))\

#####读入职工个人信息
worker=sc.textFile('/mif/mode_ac01_310.txt')
####(个人编号,(性质，出生日期(yyyy-mm-dd),性别))
worker=worker.map(lambda line:line.encode("utf-8").split(","))\
    .map(lambda line:(line[1],(line[3:6])))\
    .sortByKey()
#
######(个人编号,(年份，工作性质，出生日期,性别,年度工资，累计住院天数,住院次数，累计个人支付 20))
######(个人编号,(年份，年龄，离退状态,工作性质,性别,年度工资，累计住院天数,住院次数，累计个人支付 20))
workerHospitalCharge=hospitalCharge.join(worker) \
    .map(lambda (key,value):(key, (value[0][0],value[1][0], value[1][1],value[1][2], value[0][1],value[0][2],value[0][3],value[0][4])))\
    .map(ageComputed)\
    .sortByKey()

# #####读入职工慢性病登记信息
chroric=sc.textFile("/mif/data_new/worker_chroric_regist.txt")
chroric=chroric.map(lambda line:line.encode('utf-8').split(','))\
    .filter(lambda line:line[3][-2]<='15' and line[4][-2:]>='15')\
    .map(lambda line:(line[1],'1'))\
    .sortByKey()

######(个人编号,(年份，年龄，离退状态,工作性质,性别,年度工资，累计住院天数,住院次数，累计个人支付 20))
######(个人编号,(年份，年龄，离退状态,工作性质,性别,年度工资，是否患有慢性病，累计住院天数,住院次数，累计个人支付 20))
result=workerHospitalCharge.leftOuterJoin(chroric)\
    .map(chroricProcessing) \
    .sortByKey()


######(个人编号,(年份，年龄，离退状态,工作性质,性别,年度工资，是否患有慢性病，累计住院天数,住院次数，累计个人支付 20))
out = open('output/ins_processing.csv', 'w+')
for (key,value) in result.collect():
  line= reduce(lambda a, b: "%s,%s"%(a,b),value).encode("utf-8")
  out.write("%s,%s\n"%(key,line))
out.close()