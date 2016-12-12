from operator import add

# base operators

def isClean(x):
    try: 
        return float(x[0])<>0 and float(x[1])<>0 and float(x[2])<>0 and float(x[3])<>0
    except ValueError:
        return None

def clean(rdd):
    return rdd\
      .map(lambda x: x.encode("ascii")\
      .split(",")[6:10])\
      .filter(lambda x : len(x)==4)\
      .filter(isClean)\
      .map(lambda x: (x[0]+":"+x[1]+":"+x[2]+":"+x[3],1))
                   
def topK(rdd,k):
    return rdd.reduceByKey(add).top(k,key=lambda item: item[1] )


# streaming operator

def sclean(dstream):
    return clean(dstream)

def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)  # add the new values with the previous running count to get the new count

def scount(dstream):    
    return dstream.updateStateByKey(updateFunction)
