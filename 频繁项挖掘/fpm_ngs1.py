#-*-coding:utf-8-*-
from pyspark import SparkContext
from py4j.java_gateway import JavaGateway
sc = SparkContext()
gateway = JavaGateway()                        # connect to the JVM
object= gateway.jvm.java.util.Object()  # create an ArrayList
from pyspark.mllib.fpm import FPGrowth

nums = set()
reader = open('/home/edu/songsong/python/freItem/output/numberOfNGS.csv')
for num in reader:
    nums.add(num.strip('\n'))

data = sc.textFile("/mif/data/worker_hospital_detail.txt")
data = data.map(lambda line: line.split(','))
# num 0 ,medical_name 2 ,count 4
data_ngs = data.filter(lambda line: line[0] in nums and len(line) > 4)
#basket
data_bkt_withNum = data_ngs.map(lambda line: ((line[0], line[2]), 1)) \
    .reduceByKey(lambda a, b: a) \
    .map(lambda (k, v): (k[0], [k[1]])) \
    .reduceByKey(lambda a, b: a + b)

data_bkt=data_bkt_withNum.map(lambda (k, v): v)
data_bkt.cache()
model = FPGrowth.train(data_bkt, 0.6)
fitems = model.freqItemsets().collect()
out = open('/home/edu/songsong/python/freItem/output/ngs.csv', 'w')
for itemset in fitems:
    line = reduce(lambda a, b: "%s\t%s"%(a,b), itemset.items).encode("utf-8")
    out.write("%d\t%s\n" % (itemset.freq,line))
out.close()

####生成关联规则
rules = sorted(model._java_model.generateAssociationRules(0.6).collect(),
    key=lambda x: x.confidence(), reverse=True)
for rule in rules[:200]:
    # rule variable has confidence(), consequent() and antecedent()
    # methods for individual value access.
    object=rule.consequent()
    print object.toString()
    print "%s,%s,%s"%(rule.confidence(),rule.antecedent())
