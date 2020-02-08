import pyspark
import time


def is_good_line(line):
	try:
		fields = line.split(",")
		if len(fields) != 7:
			return False     

		float(fields[3])
		return True    
	except:
		return False  


def is_good_line2(line):
	try:
		fields = line.split(",")
		if len(fields) != 5:
			return False     

		fields[0]
		return True    
	except:
		return False

sc = pyspark.SparkContext()
transLines = sc.textFile("hdfs://.../data/ethereum/transactions")
clean_transLines = transLines.filter(is_good_line).map(lambda l: l.split(","))
features = clean_transLines.map(lambda f: (f[2], float(f[3])/10**18))
resultTrans = features.reduceByKey(lambda a,b: a+b)

contractLine = sc.textFile("hdfs://.../data/ethereum/contracts")
clean_contractLines = contractLine.filter(is_good_line2).map(lambda contractL: contractL.split(","))
featuresContracts = clean_contractLines.map(lambda featuresContracts: (featuresContracts[0], (featuresContracts[1],featuresContracts[1])))

joined = resultTrans.join(featuresContracts)
top10 = joined.takeOrdered(10, key = lambda x: -x[1][0])
top10 = sc.parallelize(top10)
top10.saveAsTextFile("joined-spark")
