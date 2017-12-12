#-*-coding:utf-8-*-
'''
统计每年参保人员的具体情况(2000,2001,2002,...2015)
'''
from pyspark import SparkContext
def preprocessing((key,value)):
    try:
        year=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        if key[0]=='2000':
            year[0]=1
        elif key[0]=='2001':
            year[1]=1
        elif key[0]=='2002':
            year[2]=1
        elif key[0]=='2003':
            year[3]=1
        elif key[0]=='2004':
            year[4]=1
        elif key[0]=='2005':
            year[5]=1
        elif key[0] == '2006':
            year[6] = 1
        elif key[0]=='2007':
            year[7]=1
        elif key[0]=='2008':
            year[8]=1
        elif key[0]=='2009':
            year[9]=1
        elif key[0]=='2010':
            year[10]=1
        elif key[0]=='2011':
            year[11]=1
        elif key[0]=='2012':
            year[12]=1
        elif key[0]=='2013':
            year[13]=1
        elif key[0]=='2014':
            year[14]=1
        elif key[0]=='2015':
            year[15]=1
        return (key[1],year)
    except Exception:
        return (str(99999),0)

def computed(line1,line2):
    for i in range(0,len(line1)):
        line1[i]=line1[i]+line2[i]
    return line1


sc=SparkContext()
data=sc.textFile('/mif/data_new/mode_ac43_310.txt')
###((年份，个人编号),次数))
###(个人编号，历年参保情况)
data=data.map(lambda line:line.encode('utf-8').split(','))\
    .filter(lambda line:line[3]=='310')\
    .map(lambda line:((line[2][0:4],line[0]),1))\
    .reduceByKey(lambda a,b:a+b)\
    .map(preprocessing)\
    .filter(lambda (key,value):isinstance(value,int)==False)\
    .reduceByKey(computed)\
    .sortByKey()

###(个人编号，历年参保情况)
out=open('output/populationChange.csv','w+')
for (key,value) in data.collect():
    line = reduce(lambda a, b: "%s,%s"%(a,b),value).encode("utf-8")
    out.write("%s,%s\n" % (key,line))
out.close()


