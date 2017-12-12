#-*-coding:utf-8 -*-
'''
分年度比较，不同等级医院均次住院费用、均次统筹费用支出记录
'''
from pyspark import SparkContext
import sys

sc=SparkContext()
reload(sys)
sys.setdefaultencoding('utf-8')

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

###住院费用
data=sc.textFile('/mif/data_new/worker_hospital.txt')
##((年份，医院等级)，（住院总费用，统筹账户支付,住院人次))
###((年份，（医院等级，均次住院费用，均次统筹账户支付))
###只选取2010-2015年的情况，同时不考虑异地的情况
###对同一年的结果进行字符串的连接
hospital=data.map(precessing)\
    .map(lambda line:(('20'+line[21][-2:],line[5]),(float(line[6]),float(line[17]),1)))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2])) \
    .sortByKey()\
    .filter(lambda (key,value):cmp(key[0],"2009")>0 and cmp(key[0],"2017")<0 and key[1]!='5')\
    .map(lambda (key,value):(key[0],(key[1],value[0]/value[2],value[1]/value[2])))\
    .sortByKey()

out=open('output/hospitalizationFeesBygradeCon.csv','w+')
####(（年份）,（医院等级+均次住院费用+均次统筹账户支付))
for(key,value)in hospital.collect():
    out.write('%s,%s,%.2f,%.2f\n' % (key,value[0],value[1],value[2]))
out.close()

