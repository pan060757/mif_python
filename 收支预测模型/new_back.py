#-*-coding:utf-8 -*-
'''
统计每年新参保和退保的人数(从2000年开始到2015年)
'''
year={}
new={}
for i in range(1,16):
    new[i]=0
back={}
for i in range(1,16):
    back[i]=0

input=open("output/populationChange.csv")
for line in input:
    line = line.split(',')
    for i in range(0,16):
        if line[i]=='0' and line[i+1]=='1':
            new[i+1]=new[i+1]+1
        if line[i]=='1' and line[i+1]=='0':
            back[i+1]=back[i+1]+1
out=open("output/new_back.csv","w+")
out.write("每年新增参保人员数目:\n")
for key,value in new.items():
    out.write("%s,%s\n"%(key,value))

out.write("每年退保人员数目:\n")
for key,value in back.items():
    out.write("%s,%s\n"%(key,value))
input.close()
out.close()