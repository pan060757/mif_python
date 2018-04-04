#-*coding:utf-8 -*-
'''
数据集准备（提取2015年的住院数据）(单次住院费用)
'''
import re

import datetime
from pyspark import SparkContext
import sys

sc = SparkContext()
reload(sys)
sys.setdefaultencoding("utf-8")


####将疾病分成18个大类
def diseaseProcessing(line):
    ### 医院等级的划分
    line = line.encode("utf-8").split(",")
    disease = line[27]
    if 'A' in disease or 'B' in disease:
        return ('20'+line[21][-2:],( '1', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',float(line[17])))
    elif 'C' in disease or ('D' in disease and cmp(disease, 'D48') < 0):
        return ('20'+line[21][-2:],( '0', '1', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',float(line[17])))
    elif 'D' in disease and cmp(disease, 'D48') >= 0:
        return ('20'+line[21][-2:],
                ( '0', '0', '1', '0', '0', '0', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',float(line[17])))
    elif 'F' in disease:
        return ('20'+line[21][-2:],
                ('0', '0', '0', '1', '0', '0', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',float(line[17])))
    elif 'G' in disease:
        return ('20'+line[21][-2:],
                ('0', '0', '0', '0', '1', '0', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',float(line[17])))
    elif 'H' in disease and cmp(disease, 'H59') <= 0:
        return ('20'+line[21][-2:],
                ( '0', '0', '0', '0', '0', '1', '0', '0', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',float(line[17])))
    elif 'H' in disease and cmp(disease, 'H60') >= 0:
        return ('20'+line[21][-2:], ('0', '0', '0', '0', '0', '0', '1', '0', '0', '0', '0','0', '0', '0', '0', '0', '0', '0',float(line[17])))
    elif 'I' in disease:
        return ('20'+line[21][-2:],
                ('0', '0', '0', '0', '0', '0', '0', '1', '0', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',float(line[17])))
    elif 'J' in disease:
        return ('20'+line[21][-2:],
                ('0', '0', '0', '0', '0', '0', '0', '0', '1', '0', '0',
                 '0', '0', '0', '0', '0', '0', '0',float(line[17])))
    elif 'K' in disease:
        return ('20'+line[21][-2:],
                ('0', '0', '0', '0', '0', '0', '0', '0', '0', '1', '0',
                 '0', '0', '0', '0', '0', '0', '0',float(line[17])))
    elif 'L' in disease:
        return ('20'+line[21][-2:],
                ('0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '1',
                 '0', '0', '0', '0', '0', '0', '0',float(line[17])))
    elif 'M' in disease:
        return ('20'+line[21][-2:],
                ('0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '1',
                 '0', '0', '0', '0', '0', '0', '0',float(line[17])))
    elif 'N' in disease:
        return ('20'+line[21][-2:],
                ('0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                 '0', '1', '0', '0', '0', '0', '0',float(line[17])))
    elif 'O' in disease:
        return ('20'+line[21][-2:],
                ('0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                 '0', '0', '1', '0', '0', '0', '0',float(line[17])))
    elif 'P' in disease:
        return ('20'+line[21][-2:],
                (0, 0, 0,0, 0, 0, 0,0, 0, 0, 0,0, 0, 1, '0', '0', '0',float(line[17])))
    elif 'Q' in disease:
        return ('20'+line[21][-2:],
                (0, 0, 0,0, 0, 0, 0,0, 0, 0, 0,0, 0,0,0,1, 0, 0,float(line[17])))
    elif 'R' in disease:
        return ('20'+line[21][-2:],
                (0, 0, 0,0, 0, 0, 0,0, 0, 0, 0,0, 0,0,0,0, 1, 0,float(line[17])))
    elif 'S' in disease or 'T' in disease:
        return ('20'+line[21][-2:], (0, 0, 0,0, 0, 0, 0,0, 0, 0, 0,0, 0,0,0,0,0,1,float(line[17])))
    else:
        return ('20'+line[21][-2:],( 0, 0, 0,0, 0, 0, 0,0, 0, 0, 0,0, 0,0,0,0,0,0,float(line[17])))

data = sc.textFile("/mif/data_new/worker_hospital.txt")
data=data.map(diseaseProcessing) \
    .filter(lambda (key, value): isinstance(value,int) == False) \
    .sortByKey()
