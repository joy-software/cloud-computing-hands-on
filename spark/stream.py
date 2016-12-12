import time
import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from utils import *

sc = SparkContext('local', 'test')
# sc.setLogLevel("ERROR")
sc.setCheckpointDir("/tmp") # for stable state        
ssc = StreamingContext(sc,1)

rddQ = []
# rddQ.append(sc.textFile("data/split/aa"))
for filename in os.listdir("data/split"):
    rddQ.append(sc.textFile("data/split/"+filename))
    

result = []
def update_result(rdd):
    global result
    print result
    result = rdd.top(10)

 # processing
dstream = ssc.queueStream(rddQ)
dstream = sclean(dstream)
dstream = scount(dstream)
dstream\
  .map(lambda x: (x[1],x[0]))\
  .foreachRDD(lambda rdd: update_result(rdd))

ssc.start()
ssc.awaitTerminationOrTimeout()
ssc.stop()

for (k,v) in result:
    print(str(k)+" "+str(v))

