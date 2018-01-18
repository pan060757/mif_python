#-*-coding:utf-8 -*-
'''
解析临床路径文档（doc文件）
'''
#读取docx中的文本代码示例
import docx
import re
import os
import xlwt
import string

wb=xlwt.Workbook()
ws=wb.add_sheet('Test')
# 遍历指定目录，显示目录下的所有文件名
def eachFile(filepath):
    pathDir =  os.listdir(filepath)
    result=[]
    for filename in pathDir:
        child = os.path.join('%s/%s' % (filepath, filename))
        print(child) # .decode('gbk')是解决中文显示乱码问题
        file=docx.Document(child)
        fullText=""
        paras =file.paragraphs
        for p in paras:
            fullText=fullText+p.text     ###得到文档的全部内容
        diseaseName=re.findall(r"(\D.*?)临床路径",filename)      ##得到疾病的名称
        person = re.findall(r"适用对象(.*?)。",fullText)         ###可以识别中文
        days = re.findall(r"标准住院日为([0-9\-]*?)[天日]",fullText)         ###可以识别中文
        trement = re.findall(r"必[须需]的检查项目：(.*?)。",fullText)         ###可以识别中文
        alternative = re.findall(r"根据患者情况可选择：(.*?)。",fullText)         ###可以识别中文
        temp=""
        if(len(alternative)>0):
            temp=diseaseName[0]+"\t"+person[0]+"\t"+str(days[0])+"\t"+trement[0]+"\t"+alternative[0]
        else:
            temp = diseaseName[0] + "\t" + person[0] + "\t" + str(days[0]) + "\t" + trement[0] + "\t" + ""
        result.append(temp)
    for i in range(0,len(result)):
        value = result[i].split("\t")
        for j in range(0,len(value)):
            ws.write(i,j,value[j])
    wb.save('trement.xls')         ###将结果写入Excel文件

#获取文档对象
filePath="output"
eachFile(filePath)

