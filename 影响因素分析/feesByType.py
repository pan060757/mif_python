#-*-coding:utf-8 -*-
'''
统计病人一次住院过程中，各个费用支出情况（总费用，药品费、诊疗费、床位费，手术费，护理费、材料费）
'''
from pyspark import SparkContext

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
sc = SparkContext()
def proprecessing(line):
    try:
        line=line.split(',')
        if len(line)>7:
            feesList=[0,0,0,0,0,0,0]
            num=line[0]        #####就医序号
            id=line[1]         #####三大目录id
            name=line[2]       ####药品/诊疗名称
            if line[7]=="":
                line[7]='0'
            fees=float(line[7])
            feesList[0]=fees
            if id in medicine_broadcast.value:
                feesList[1]=fees          ###药品费
            elif "床位".decode("utf-8") in name:
                feesList[3]=fees          ###床位费
            elif "手术".decode("utf-8") in name:
                feesList[4]=fees          ###手术费
            elif "护理".decode("utf-8") in name:
                feesList[5]=fees          ###护理费费
            elif "材料".decode("utf-8") in name:
                feesList[6]=fees      ###材料费
            else:
                feesList[2]=fees          ###诊疗费
            return(num,feesList)
        else:
            return (str(999999),0)
    except Exception:
        return(str(999999),0)

#####读入药品目录
reader = open('/home/edu/mif/python/ss/mif/drug_disease/output/medicine.txt')
medicine=set()
for num in reader:
    medicine.add(num.strip("\n").split(',')[0])
medicine_broadcast=sc.broadcast(medicine)

#####读入住院详细信息
data = sc.textFile("/mif/data_new/worker_hospital_detail.txt")
data = data.map(proprecessing)\
    .filter(lambda (key,value):(isinstance(value,int)==False))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2],a[3]+b[3],a[4]+b[4],a[5]+b[5],a[6]+b[6]))\
    .sortByKey()

##（就医序号,(总费用，药品费、诊疗费、床位费，手术费，护理费、材料费))
out = open('output/feesByType.csv', 'w+')
for (key,value) in data.collect():
    out.write("%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n" % (key,value[0],value[1],value[2],value[3],value[4],value[5],value[6]))
out.close()

