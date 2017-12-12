#-*-coding:utf-8 -*-
'''
统计平均工资的分布情况
'''
from pyspark import SparkContext
import string
def dataToKV(line):
    splitLine = line.encode('utf-8').split(',')
    try:
      	num=splitLine[0]
        year = splitLine[2][0:4]
        type = splitLine[3]
        if type == '310':
            return (year+","+num, line)
        else:
            return (999999,0)
    except Exception, e:
        return (999999, 0)

def removeDupl(line1,line2):
    return line1

def wageToKV(line):
    splitLine = line.split(',')
    year=splitLine[0]
    wage=float(splitLine[1])/12
    return (year, wage)

def reformat1((yearNum,line)):
    year=yearNum.split(",")[0]
    return(year,line)

def lessThanW((year, (line, avg_wage))):
    splitLine = line.split(',')
    wage = string.atof(splitLine[5])
    if (wage<avg_wage):
        return (year,1)
    else:
        return (year,0)

def lessThan8W((year,(line,avg_wage))):
    splitLine = line.split(',')
    wage = string.atof(splitLine[5])
    if (wage<0.8*avg_wage):
        return (year,1)
    else:
        return (year,0)

def moreThan3W((year,(line,avg_wage))):
    splitLine=line.split(',')
    wage=string.atof(splitLine[5])
    if(wage>=3*avg_wage):
	    return (year,1)
    else:
	    return (year,0)

def moreThan2W((year,(line,avg_wage))):
    splitLine=line.split(',')
    wage=string.atof(splitLine[5])
    if(wage>=2*avg_wage):
        return (year,1)
    else:
        return (year,0)

def Wto3W((year,(line,avg_wage))):
    splitLine=line.split(',')
    wage=string.atof(splitLine[5])
    if(wage>avg_wage and wage<3*avg_wage):
        return (year,1)
    else:
	    return (year,0)

sc = SparkContext()
data = sc.textFile("/mif/mode_ac43_310.txt")
wage = sc.textFile("/mif/avgWage2.txt")
data = data.map(dataToKV)\
    .filter(lambda (key,value):(isinstance(key,int)==False))\
    .reduceByKey(removeDupl)\
    .map(reformat1)
wage = wage.map(wageToKV)
data = data.join(wage)
####低于平均工资的人数

data1=data.map(lessThanW).reduceByKey(lambda a,b:a+b).sortByKey()
####介于平均工资和3倍平均工资以内
data2=data.map(Wto3W).reduceByKey(lambda a,b:a+b).sortByKey()
####高出平均工资的人数
data3=data.map(moreThan3W).reduceByKey(lambda a,b:a+b).sortByKey()

result=data1.join(data2).\
    map(lambda (key,value):(key,(value[0],value[1])))\
    .join(data3).sortByKey()

out = open('output/wagePopulation.csv', 'w')
for (key, value) in result.collect():
    out.write("%s,%d,%d,%d,%d,%.2f,%.2f,%.2f\n"%(key,value[0][0],value[0][1],value[1],(value[0][0]+value[0][1]+value[1]),
                                  value[0][0]*1.0/(value[0][0]+value[0][1]+value[1]),
                                  value[0][1]*1.0/(value[0][0]+value[0][1]+value[1]),
                                  value[1]*1.0/(value[0][0]+value[0][1]+value[1])))
out.close()
