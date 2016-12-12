from operator import add

def clean(rdd):
    return rdd\
      .map(lambda x: x.encode("ascii")\
      .split(",")[6:10])\
      .filter(lambda x : len(x)==4)\
      .filter(lambda x : float(x[0])<>0 and float(x[1])<>0 and float(x[2])<>0 and float(x[3])<>0)\
      .map(lambda x: (x[0]+":"+x[1]+":"+x[2]+":"+x[3],1))
      
def topK(rdd,k):
    return rdd.reduceByKey(add).top(k,key=lambda item: item[1] )
