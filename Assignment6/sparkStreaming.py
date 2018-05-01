
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
import requests
from operator import add

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApplication")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# sc.setLogLevel("INFO")
# sc.setLogLevel("ERROR")
sc.setLogLevel("FATAL")


# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)


# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)


# split each tweet into words
words = dataStream.window(120, 6).map(lambda line: line.split(" "))

followers = words.map(lambda x: (x[0], int(x[1])))
#    .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))


def aggregate_count(new_values, total_sum):
    return new_values

# adding the count of each hashtag to its last count
#inorder_followers = followers.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

topFollowers = followers.updateStateByKey(aggregate_count)\
                .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

# Debuging code 
topFollowers.pprint(20)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()


