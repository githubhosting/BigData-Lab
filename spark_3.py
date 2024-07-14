import sys
from pyspark import SparkContext

if (len(sys.argv) != 3):
    print("Provide Input File and Output Directory")
    sys.exit(0)

sc = SparkContext()
f = sc.textFile(sys.argv[1])
temp = f.map(lambda x: (x.split('\t')[3], float(x.split('\t')[8])))
total = temp.reduceByKey(lambda a, b: a+b)
total.saveAsTextFile(sys.argv[2])
