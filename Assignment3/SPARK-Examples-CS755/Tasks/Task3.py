from __future__ import print_function
import sys
import datetime
from operator import add
from pyspark import SparkContext

def main(inputTaxi, inputPoi, output):
    if len(sys.argv) != 4:
            print("Please supply taxi input, poi input, and output arguments", file=sys.stderr)
            exit(-1)    
            
    # remove lines if they don't have 16 values
    # also if values are empty.
    def correctFormat(listline):
        if(len(listline) == 17):
            try:
                time = listline[3]
                longi = float(listline[8].strip())
                lati  = float(listline[9].strip())
            except Exception:
                return

            if longi and lati and time: #is not empty and
                if longi !=0.0 and lati != 0.0: #if value is not 0.
                    return listline
    
    
    def isfloat(value):
        try:
            float(value)
            return True
        except:
            return False

    def correctPoint(listplace):
        lati  = listplace[0]
        longi = listplace[1]
        nameplace = listplace[2]
        
        # if lati and logi is float and nameofplace is not empty.
        if isfloat(lati) and isfloat(longi) and nameplace:
            return listplace
        
    def getCellID(lat, lon):
        return (str(round(lat, 2)) + ' & ' +str(round(lon, 2)))
    
    def setDate(value):
        dateform = value.split(' ')
        return dateform[0]

    def setTime(value):
        dateform = value.split(' ')
        # round to hour
        return (dateform[1].split(':'))[0]
        
    def getgrid(value):
        return value[0][0]
    
    def getDate(value):
        return value[0][1]
    
    def getTime(value):
        return value[0][2]
    
    # get average taxi
    """
    grid-cell+hour of drop-offs
    compute 24 average of everyday for each grid since a day is 24 hours.
    key has to be somewhat time and grid cell.
    """

    # computer drop-off with reduce by same date and same hours
    """
    make a format (grid-cell+date+hour of drop-offs)
    """
    dateHourTaxi = inputTaxi.map(lambda x: x.decode("iso-8859-1").split(',')) \
    .filter(correctFormat) \
    .map(lambda x: ( (getCellID(float(x[9]), float(x[8])), setDate(x[3]), setTime(x[3]) ), 1 ) ) \
    .reduceByKey(add)

    # get how many date on same place same time.
    # this is for comput average
    gridDateTaxi = dateHourTaxi.map(lambda x: ((getgrid(x), getTime(x)), 1) ) \
    .reduceByKey(add) \
    .collectAsMap()

    def lookforDate(value):
        numberofdrop = gridDateTaxi.get(value)
        if numberofdrop:
            return numberofdrop
        return 1
    
    
    averageTaxi = dateHourTaxi.map(lambda x: ((getgrid(x), getTime(x)), x[1])) \
    .reduceByKey(add) \
    .map(lambda x: (x[0], x[1]/lookforDate(x[0])) ) \
    .collectAsMap()

    def lookforAverage(value):
        lookupv = (getgrid(value), getTime(value))
        numberofdrop = averageTaxi.get(lookupv)
        if numberofdrop:
            return numberofdrop
        return 0
    
    top20 = dateHourTaxi.map(lambda x: ((x[0][0], x[0][1], x[0][2], x[1]), x[1]/lookforAverage(x) )) \
    .map(lambda x: (x[1], x[0])) \
    .top(20)
    
    #print ("top20")
    #print (top20)

    
    placelist = inputPoi.map(lambda x: x.decode("iso-8859-1").split('||')) \
    .filter(correctPoint) \
    .map(lambda x: (getCellID(float(x[0]), float(x[1])), x[2]) ) \
    .reduceByKey(lambda a,b : a + ', ' +b) \
    .collectAsMap()
    
    
    def lookforplaces(value):
        lookupv = placelist.get(value)
        if lookupv:
            return str(lookupv.encode('utf-8'))
        return ''

    top20set = set()

    for eachitem in top20:
        # make a tuple of 6 item. get the name of point of interest
        temptuple = (eachitem[1][0], eachitem[1][2], eachitem[1][1], eachitem[0], eachitem[1][3], lookforplaces(eachitem[1][0]))
        # add to set
        top20set.add(temptuple)
    
    # re-parallelize, and save to a text file (probably a better way to do this, but didn't want the hassle of using boto3 for s3 saving)
    sc.parallelize(top20set).saveAsTextFile(output)
    

if __name__ == "__main__":
    # set up the spark context
    sc = SparkContext(appName="PythonTaxiTask3")

    # set the main arguments
    inputTaxi = sc.textFile(sys.argv[1], 1,  use_unicode=False)
    inputPoi  = sc.textFile(sys.argv[2], 1, use_unicode=False)
    output    = sys.argv[3]

    # kick off the main function
    main(inputTaxi, inputPoi, output)
    
   