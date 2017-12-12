#-coding:utf-8-*-
'''
年龄结构对人均统筹费用的影响
'''
import string
from pyspark import SparkContext


###对于进行连接的数据进行处理，计算参保人员年龄信息
def processing_4((key,value)):
    try:
        current_year=value[0]
        birth_year=value[1][0][0:4]
        age=string.atoi(current_year)-string.atoi(birth_year)         ###计算在职员工当前缴费时的年龄
        return (key,(current_year,age))
    except Exception:
        return (str(999999),0)

#####对参保人员进行年龄段的划分
def ageDivided((key,value)):
    year =value[0]
    age =value[1]
    if age in range(0, 20):
        return (year + ',' + '0',1)
    elif age in range(20, 30):
        return (year + ',' + '1',1)
    elif age in range(30, 40):
        return (year + ',' + '2',1)
    elif age in range(40, 50):
        return (year + ',' + '3',1)
    elif age in range(50, 60):
        return (year + ',' + '4',1)
    elif age in range(60,70):
        return (year + ',' + '5',1)
    elif age in range(70, 80):
        return (year + ',' + '6',1)
    elif age in range(80, 90):
        return (year + ',' + '7',1)
    elif age in range(90, 100):
        return (year + ',' + '8',1)
    elif age in range(100, 150):
        return (year + ',' + '9',1)
    else:
        return (str(999999), 0)


###对于进行连接的数据进行处理，计算住院患者年龄信息
##（个人编号,（当前年份，年龄，统筹费用支出））
def ageComputed((key,value)):
    try:
        current_year=value[0][0]
        birth_year=value[1][0][0:4]
        age=string.atoi(current_year)-string.atoi(birth_year)         ###计算在职员工当前缴费时的年龄
        return (key,(current_year,age,value[0][1]))
    except Exception:
        return (str(999999),0)


####((年份,年龄段),(住院统筹费用支出，住院人数))
def ageDividedOfHfees((key,value)):
    year =value[0]
    age =value[1]
    if age in range(0, 20):
        return (year + ',' + '0',(value[2],1))
    elif age in range(20, 30):
        return (year + ',' + '1',(value[2],1))
    elif age in range(30, 40):
        return (year + ',' + '2',(value[2],1))
    elif age in range(40, 50):
        return (year + ',' + '3',(value[2],1))
    elif age in range(50, 60):
        return (year + ',' + '4',(value[2],1))
    elif age in range(60, 70):
        return (year + ',' + '5',(value[2],1))
    elif age in range(70, 80):
        return (year + ',' + '6',(value[2],1))
    elif age in range(80, 90):
        return (year + ',' + '7',(value[2],1))
    elif age in range(90, 100):
        return (year + ',' + '8',(value[2],1))
    elif age in range(100,150):
        return (year + ',' + '9',(value[2],1))
    else:
        return (str(99999), 0)

sc=SparkContext()
data=sc.textFile('/mif/data_new/worker_hospital.txt')
####（（人员编号,年份),统筹支付）
####（（人员编号)(年份,统筹支付)）
data1=data.map(lambda line:line.encode('utf-8').split(','))\
    .filter(lambda line:line[17]!="")\
    .map(lambda line:((line[1],'20'+line[21][-2:]),float(line[17])))\
    .reduceByKey(lambda a,b:a+b)\
    .map(lambda (key,value):(key[0],(key[1],value)))\
    .sortByKey()

####(number,(birthdate,sex))
data2=sc.textFile('/mif/data_new/worker.txt')
data2=data2.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:(line[1],line[4:6]))

####（（人员编号)(年份,统筹支付,出生日期，性别)）
####((年份,年龄段),(住院统筹费用支出，住院人数))
####((年份,年龄段),人均统筹费用支出)
fees=data1.join(data2)\
    .map(ageComputed)\
    .filter(lambda(key,value):(isinstance(value,int)==False))\
    .map(ageDividedOfHfees) \
    .filter(lambda (key, value): (isinstance(value, int) == False)) \
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))\
    .map(lambda (key,value):(key,value[0]/value[1]))\
    .sortByKey()


data3=sc.textFile('/mif/data_new/mode_ac43_310.txt')
##每步map对应的键值对转化
#(year+','+number,1)
#(number,year)
data3=data3.map(lambda line:line.encode('utf-8').split(','))\
    .filter(lambda line:line[3]=='310'and line[4]=='10')\
    .map(lambda line:(line[2][0:4]+','+line[0],1))\
    .reduceByKey(lambda a,b:a+b)\
    .map(lambda (key,value):(key.split(',')[1],key.split(',')[0]))\
    .sortByKey()

#(year+','+(年龄段)+','+sex,1)
#（(year,年龄段)，number))
number=data3.join(data2)\
    .map(processing_4)\
    .filter(lambda (key,value):(isinstance(value,int)==False))\
    .map(ageDivided)\
    .reduceByKey(lambda a,b:a+b)\
    .filter(lambda(key,value):(len(key.split(','))>1))\
    .sortByKey()


#（(year,年龄段)，（人均住院统筹费用支出,参保人数))
result=fees.join(number)\
    .sortByKey()

####(（年份,年龄段),(人均住院统筹支出，参保人数,人均住院统筹支出*参保人数)))
out=open('output/age_AvghgroupFees.csv','w+')
for (key,value) in result.collect():
    out.write("%s,%.2f,%d,%.2f\n"%(key,value[0],value[1],value[0]*value[1]))
out.close()