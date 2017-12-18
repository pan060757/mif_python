#-*-coding:utf-8 -*-
'''
参保人员年龄（未进行年龄段划分）
'''
from pyspark import SparkContext
import string

###对于进行连接的数据进行处理，计算参保人员年龄信息
###(（年份，年龄,性别），1）)
def processing_4((key,value)):
    try:
        current_year=value[0]
        birth_year=value[1][0][0:4]
        sex=value[1][1]
        age=string.atoi(current_year)-string.atoi(birth_year)         ###计算在职员工当前缴费时的年龄
        return ((current_year,age,sex),1)
    except Exception:
        return (str(999999),0)

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

#（(year,年龄，性别)，number))
#（(year,年龄)，(性别,number))
#将分性别的数据连接在一起
data=data1.join(data2)\
    .map(processing_4)\
    .filter(lambda (key,value):(isinstance(key,str)==False)) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda (key,value):((key[0],key[1]),(key[2],value)))\
    .reduceByKey(lambda a,b:a+b)\
    .sortByKey()


####((年份,年龄，性别，缴费人数))
out=open('output/ageOfCharge.csv','w+')
for (key,value) in data.collect():
    line=reduce(lambda a,b:"%s,%s"%(a,b),value).encode("utf-8")
    out.write("%s,%d,%s\n"%(key[0],key[1],line))
out.close()