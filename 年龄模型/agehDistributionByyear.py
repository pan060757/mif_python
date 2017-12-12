#-*-coding:utf-8 -*-
'''
对每一年住院的参保人员的年龄分布
'''
import string

from pyspark import SparkContext

###对于进行连接的数据进行处理，计算参保人员年龄信息
def ageComputed((key,value)):
    try:
        current_year=value[0][0]
        birth_year=value[1][0][0:4]
        age=string.atoi(current_year)-string.atoi(birth_year)         ###计算在职员工当前缴费时的年龄
        return (key,(current_year,age,value[0][1],value[0][2]))
    except Exception:
        return (str(999999),0)

def ageDivided((key,value)):
    year =value[0]
    age =value[1]
    if age in range(0, 20):
        return (year + ',' + '0',(value[2],value[3]))
    elif age in range(20, 30):
        return (year + ',' + '1',(value[2],value[3]))
    elif age in range(30, 40):
        return (year + ',' + '2',(value[2],value[3]))
    elif age in range(40, 50):
        return (year + ',' + '3',(value[2],value[3]))
    elif age in range(50, 60):
        return (year + ',' + '4',(value[2],value[3]))
    elif age in range(60, 70):
        return (year + ',' + '5',(value[2],value[3]))
    elif age in range(70, 80):
        return (year + ',' + '6',(value[2],value[3]))
    elif age in range(80, 90):
        return (year + ',' + '7',(value[2],value[3]))
    elif age in range(90, 100):
        return (year + ',' + '8',(value[2],value[3]))
    elif age in range(100,150):
        return (year + ',' + '9',(value[2],value[3]))
    else:
        return (str(99999), 0)

sc=SparkContext()
data=sc.textFile('/mif/data_new/worker_hospital.txt')
####（（人员编号,年份)(统筹支付，次数)）
####（（人员编号)(年份,统筹支付，次数)）
data1=data.map(lambda line:line.encode('utf-8').split(','))\
    .filter(lambda line:line[17]!="")\
    .map(lambda line:((line[1],'20'+line[21][-2:]),(float(line[17]),1)))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))\
    .map(lambda (key,value):(key[0],(key[1],value[0],value[1])))\
    .sortByKey()

####(number,(birthdate,sex))
data2=sc.textFile('/mif/data_new/worker.txt')
data2=data2.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:(line[1],line[4:6]))

result=data1.join(data2)\
    .map(ageComputed)\
    .filter(lambda(key,value):(isinstance(value,int)==False))\
    .map(ageDivided) \
    .filter(lambda (key, value): (isinstance(value, int) == False)) \
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))\
    .sortByKey()


####((年份,年龄段)，住院统筹费用支出，住院人次))
out=open('output/agehDistributionByyear.csv','w+')
for (key,value) in result.collect():
   out.write("%s,%.2f,%d\n"%(key,value[0],value[1]))
out.close()

