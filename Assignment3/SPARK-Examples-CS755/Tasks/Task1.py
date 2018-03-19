from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext

sc = SparkContext(appName="PythonTaxi")

def main(input, output):    
    # remove lines if they don't have 16 values
    # also if medallion or driver ID is empty.
    def correctFormat(listline):
        if(len(listline) == 17):
            if listline[0] and listline[1]:
                return listline
    
    # 1. filter out
    # 2. get only taxi ID and driver ID
    # 3. distinct items if taxi ID and driver ID are same
    # 4. after distinct, i don't need to care about driver ID anymore. replace 1 with driver ID
    # 5. add up.
    # 6. swap key and value for sorting top
    
    topTenTaxi = (
    input.map(lambda x: x.split(','))
    .filter(correctFormat)
    .map(lambda x: (x[0], x[1]))
    .distinct()
    .map(lambda x: (x[0], 1))
    .reduceByKey(add)
    .map(lambda x: (x[1],x[0]))
    .top(10)
    )

    sc.parallelize(topTenTaxi).saveAsTextFile(output)

    return topTenTaxi

if __name__ == "__main__":
    # get the input arguments
    if len(sys.argv) != 3:
        print("Please specify an input and output path", file=sys.stderr)
        exit(-1)

    input = sys.argv[1]
    output = sys.argv[2]

    # set up the spark context
    taxiLines = sc.textFile(input, 1)

    # execute the main function
    topTaxis = main(taxiLines, output)