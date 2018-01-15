#-*coding:utf-8 -*-
'''
数据集准备（提取2015年的住院数据）(单次住院费用)
'''
import re

import datetime
from pyspark import SparkContext
import sys

sc = SparkContext()
reload(sys)
sys.setdefaultencoding("utf-8")
#####住院数据预处理
###（(个人编号 1,年份),(就医序号 2，医院等级 5，住院天数，药品费 10,起付线 14,报销比例 15，统筹账户支付 17，出院病种编号 26 )))）
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
        return ((line[1],'20'+s[2]),(line[2],line[5],days,line[10],line[14],line[15],line[17],line[26]))
    else:
        return (str(999999), 1)

    ####将疾病分成18个大类
def diseaseProcessing((key,value)):
    disease = value[11]
    if 'A' in disease or 'B' in disease:
        return (key,(value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10],
                 '1', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'C' in disease or ('D' in disease and cmp(disease, 'D48') < 0):
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '1', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'D' in disease and cmp(disease, 'D48') >= 0:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '1', '0', '0', '0', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'F' in disease:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '1', '0', '0', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'G' in disease:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '1', '0', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'H' in disease and cmp(disease, 'H59') <= 0:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '1', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'H' in disease and cmp(disease, 'H60') >= 0:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '0', '1', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'I' in disease:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '0', '0', '1', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'J' in disease:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '0', '0', '0', '1', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'K' in disease:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '0', '0', '0', '0', '1', '0',
                 '0', '0', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'L' in disease:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '1',
                 '0', '0', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'M' in disease:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '1',
                 '0', '0', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'N' in disease:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                 '0', '1', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'O' in disease:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                 '0', '0', '1', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'P' in disease:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                 '0', '0', '0', '1', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'Q' in disease:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '1', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'R' in disease:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '1', '0',value[13],value[14],value[15],value[16],value[17],value[12]))
    elif 'S' in disease or 'T' in disease:
        return (key,
                (value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '1',value[13],value[14],value[15],value[16],value[17],value[12]))
    else:
        return (key,(value[0], value[1],value[2],value[3],value[4], value[5], value[6], value[7], value[8],value[9],value[10], '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',value[13],value[14],value[15],value[16],value[17],value[12]))

###(个人编号 0，年度工资 5)存在重复
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
#####(个人编号,(身份,性质，年龄,性别,年度工资，就医序号，医院等级,住院天数,药品费，起付线，报销比例，出院病种,统筹账户支付))
def ageComputed((key, value)):
    currentDate = value[13]  ###入院日期
    birthDate = value[2][0:4]  ###出生日期
    age = int(currentDate) - int(birthDate)  ##计算住院时年龄
    return (key, (value[0], value[1],age, value[3], value[4], value[5], value[6],
                  value[7],value[8],value[9],value[10],value[12],value[11]))

####是否患有慢性病
def chroricProcessing((key,value)):
    if value[1]==None:
        return (key, (value[0][0], value[0][1], value[0][2],value[0][3],value[0][4],value[0][5],value[0][6],
                      value[0][7], value[0][8], value[0][9],value[0][10],'0',value[0][11],value[0][12]))
    else:
        return (key, (value[0][0], value[0][1], value[0][2], value[0][3], value[0][4], value[0][5], value[0][6],
                      value[0][7], value[0][8], value[0][9],value[0][10],'1', value[0][11],value[0][12]))

####
def feesProcessing(line):
    try:
        line=line.encode("utf-8").split(',')
        for i in range(3,8):
            if float(line[i])<0:    ####对出现值为负数的记录
                return (str(999999),1)
        return (line[0], (line[3], line[4], line[5], line[6], line[7]))
    except Exception:
        return (str(999999),1)
####((个人编号,医院等级),(住院人次))
####(个人编号,(医院等级，住院人次))
####读入职工住院数据
###（个人编号 1,(就医序号，医院等级 5，住院天数,药品费 10,起付线 14,报销比例 15，统筹账户支付 17，出院病种编号 26
data = sc.textFile("/mif/data_new/worker_hospital.txt")
data=data.map(hospitalProcessing) \
    .filter(lambda (key, value): isinstance(value,int) == False) \
    .sortByKey()


#####读入职工缴费数据
######(个人编号，年度工资)
charge=sc.textFile("/mif/data_new/mode_ac43_310.txt")
charge=charge.map(chargeProcessing)\
    .filter(lambda (key,value):(isinstance(value,int)==False))\
    .reduceByKey(lambda a,b:a+b)\
    .filter(lambda (key,value):(value>0))\
    .sortByKey()

#####(个人编号,(年度工资，就医序号，医院等级 5，住院天数，药品费 10,起付线 14,报销比例 15，统筹账户支付 17，出院病种编号 26,入院日期)
hospitalCharge=data.join(charge)\
    .map(lambda (key,value):(key[0],(value[1],value[0][0],value[0][1],value[0][2],value[0][3],value
                             [0][4],value[0][5],value[0][6],value[0][7],key[1])))\

#####读入职工个人信息
worker=sc.textFile('/mif/mode_ac01_310.txt')
####(个人编号,(身份,性质，出生日期(yyyy-mm-dd),性别))
worker=worker.map(lambda line:line.encode("utf-8").split(","))\
    .map(lambda line:(line[1],(line[2:6])))\
    .sortByKey()
#
# #####(个人编号,(身份,性质，出生日期,性别,年度工资，就医序号，医院等级,住院天数,药品费，起付线，报销比例，统筹账户支付，出院病种))
# #####(个人编号,(身份,性质，年龄,性别,年度工资，就医序号，医院等级,住院天数,药品费，起付线，报销比例，出院病种，统筹账户支付))
workerHospitalCharge=hospitalCharge.join(worker) \
    .map(lambda (key,value):(key, (value[1][0], value[1][1],value[1][2], value[1][3],value[0][0], value[0][1],value[0][2],value[0][3],value[0][4],value[0][5],value[0][6],value[0][7],value[0][8],
                                   value[0][9])))\
    .map(ageComputed)\
    .sortByKey()

# #####读入职工慢性病登记信息
chroric=sc.textFile("/mif/data_new/worker_chroric_regist.txt")
chroric=chroric.map(lambda line:line.encode('utf-8').split(','))\
    .filter(lambda line:line[3][-2]<='15' and line[4][-2:]>='15')\
    .map(lambda line:(line[1],'1'))\
    .sortByKey()

# #####(个人编号,(身份,性质，年龄,性别,年度工资，就医序号，医院等级,住院天数,药品费，起付线，报销比例，出院病种，统筹账户支付))
# #####(个人编号,(身份,性质，年龄,性别,年度工资，就医序号，医院等级,住院天数,药品费，起付线，报销比例，出院病种，是否患有慢性病,统筹账户支付))
# #####(就医序号,(身份,性质，年龄,性别,年度工资，医院等级,住院天数,药品费，起付线，报销比例，出院病种，是否患有慢性病,统筹账户支付))
result=workerHospitalCharge.leftOuterJoin(chroric)\
    .map(chroricProcessing) \
    .map(lambda (key, value): (value[5],(value[0], value[1], value[2], value[3], value[4],value[6], value[7],
                                     value[8],value[9],value[10],value[11],value[12],value[13])))\

#####读入住院费用详细数据(其中药品费，前面已经包括)
#####（就医序号，（诊疗费、床位费，手术费，护理费、材料费））
fees=sc.textFile("/mif/data_new/feesByType.csv")
fees=fees.map(feesProcessing)\
    .filter(lambda (key,value):isinstance(value,int)==False)\
    .sortByKey()

result=result.join(fees)\
    .map(lambda(key,value):(key,(value[0][0], value[0][1], value[0][2], value[0][3], value[0][4],value[0][5],value[0][6], value[0][7],
                                     value[0][8],value[0][9],value[0][10],value[0][11],value[0][12],value[1][0], value[1][1], value[1][2], value[1][3], value[1][4])))\
    .map(diseaseProcessing)\
    .sortByKey()



#####(个人编号,(身份,性质，年龄,性别,年度工资，医院等级,住院天数,药品费，起付线，报销比例，是否患有慢性病，出院病种(18个大类),诊疗费、床位费，手术费，护理费、材料费,统筹账户支付))
out = open('output/DataOfFees.csv', 'w+')
for (key,value) in result.collect():
  line= reduce(lambda a, b: "%s,%s"%(a,b),value).encode("utf-8")
  out.write("%s,%s\n"%(key,line))
out.close()
