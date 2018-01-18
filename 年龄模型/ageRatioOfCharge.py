#-*-coding:utf-8-*-
'''
年龄结构预测
'''
import math
###读入2015年分性别年龄结构情况

age_number={}       ###存储每个年龄对应的人数
age_ratio={}        ###存储每个年龄所占的比例
death_ratio={}      ###存储每个年龄对应的死亡率情况

ageOfCharge=open('output/ageOfCharge.csv')
for line in ageOfCharge:
    line=line.split(",")
    if(line[0]=='2015'):
        age=line[1]
        number=int(line[2])
        a_ratio=float(line[4])
        age_number[age]=number
        age_ratio[age]=a_ratio

###参保人数存留情况
age_reserved={}       ##保留每个年龄的存活人数
for (key,value) in age_number.items():
    if(age_ratio[key]>1):   ###死亡率超过1的情况
        temp_value=0
        age_reserved[key] = temp_value
    else:
        temp_value=value*(1-age_ratio[key])
        age_reserved[key] = math.ceil(temp_value)


for(key,value)in age_reserved.items():
    print(key,value)

