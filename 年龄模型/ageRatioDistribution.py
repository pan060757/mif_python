#-*-coding:utf-8 -*-
'''
参保人员各个年龄段所占比例
'''
from pyspark import SparkContext
import string

###对于进行连接的数据进行处理，计算参保人员年龄信息
##((人员编号，（当前年份，年龄，性别）))
def processing_4((key,value)):
    try:
        current_year=value[0]
        birth_year=value[1][0][0:4]
        sex=value[1][1]
        age=string.atoi(current_year)-string.atoi(birth_year)         ###计算在职员工当前缴费时的年龄
        return (key,(current_year,age,sex))
    except Exception:
        return (str(999999),0)

def ageDivided((key,value)):
    year =value[0]
    age =value[1]
    sex=value[2]
    if age in range(0, 20):
        return ((year ,'0',sex),1)
    elif age in range(20, 30):
        return ((year ,'1',sex),1)
    elif age in range(30, 40):
        return ((year ,'2',sex),1)
    elif age in range(40, 50):
        return ((year ,'3',sex),1)
    elif age in range(50, 60):
        return ((year ,'4',sex),1)
    elif age in range(60,70):
        return ((year ,'5',sex),1)
    elif age in range(70, 80):
        return ((year ,'6',sex),1)
    elif age in range(80, 90):
        return ((year ,'7',sex),1)
    elif age in range(90, 100):
        return ((year ,'8',sex),1)
    elif age in range(100, 150):
        return ((year ,'9',sex),1)
    else:
        return (str(999999), 0)


###最终数据展示
def dataConstrate(value1,value2):
    return list(value1).extend(value2)
###程序入口
sc=SparkContext()
data1=sc.textFile('/mif/data_new/mode_ac43_310.txt')
##每步map对应的键值对转化
#(year+','+number,1)
#(number,year)
data1=data1.map(lambda line:line.encode('utf-8').split(','))\
    .filter(lambda line:line[3]=='310'and line[4]=='10')\
    .map(lambda line:(line[2][0:4]+','+line[0],1))\
    .reduceByKey(lambda a,b:a+b)\
    .map(lambda (key,value):(key.split(',')[1],key.split(',')[0]))\
    .sortByKey()


####(number,(indentify,xingzhi,birthdate,sex))
data2=sc.textFile('/mif/data_new/worker.txt')
data2=data2.map(lambda line:line.encode('utf-8').split(','))\
    .map(lambda line:(line[1],line[4:6]))

#(year+','+(年龄段)+','+sex,1)
#（(year,年龄段,性别)，number))
#(年份，年龄段），（性别，缴费人数）)
#分性别数据进行连接
data=data1.join(data2)\
    .map(processing_4)\
    .filter(lambda (key,value):(isinstance(value,int)==False))\
    .map(ageDivided)\
    .reduceByKey(lambda a,b:a+b)\
    .filter(lambda(key,value):(isinstance(key,str)==False))\
    .sortByKey()\
    .map(lambda(key,value):((key[0],key[1]),(key[2],value)))\
    .reduceByKey(lambda a,b:a+b)\
    .sortByKey()


####(年份,年龄段，缴费人数)
out=open('output/ageRatioDistribution.csv','w+')
for (key,value) in data.collect():
    line = reduce(lambda a, b: "%s,%s" % (a, b), value).encode("utf-8")
    out.write("%s,%s,%s\n" % (key[0],key[1], line))
out.close()