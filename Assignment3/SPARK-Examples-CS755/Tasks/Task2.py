from __future__ import print_function
import sys
import datetime
from operator import add
from pyspark import SparkContext


    
    #difference btw map and flatmap
    # map : go each and change
    # flatmap : make a new list
    
    
# filter out file within 8 ~ 11 am
def morningTime(value):
    date = value.split(' ')
    time = date[1].split(':')
    hour = int(time[0])
    if hour >= 8 and hour < 11:
        return True
    return False
  
# listline [3] = dropoff_datetime
# listline [8] = dropoff_longitude
# listline [9] = dropoff_latitude

# remove lines if they don't have 16 values
# also if values are empty.
def correctFormat(listline):
    if(len(listline) == 17):
        time = listline[3]
        longi = float(listline[8])
        lati  = float(listline[9])
        
        if longi and lati and time: #is not empty and
            if longi !=0.0 and lati != 0.0: #if value is not 0.
                if morningTime(time):
                    return listline

def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False

# listplace[0] = latitude
# listplace[1] = longitude
# listplace[2] = name of POI

def correctPoint(listplace):
    lati  = listplace[0]
    longi = listplace[1]
    nameplace = listplace[2]
    
    # if lati and logi is float and nameofplace is not empty.
    if isfloat(lati) and isfloat(longi) and nameplace:
        return listplace
        
    

def getCellID(lat, lon):
    return (str(round(lat, 2)) + " & "+str(round(lon, 2)))

#daytime.
#weekday() Return the day of the week as an integer, where Monday is 0 and Sunday is 6.
def getDay(value):
    dateform = value.split(' ')
    date = dateform[0].split('-')
    day = datetime.date(int(date[0]), int(date[1]), int(date[2]))
    return day.weekday()

# value[1] is a day. 
def isSunday(value):
    if value[1] == 0:
        return value
    


def lookforplaces(value):
    lookupv = placelist.get(value)
    if lookupv:
        return str(lookupv)
    return ''
    
    
    
if __name__ == "__main__":
    
    if len(sys.argv) != 3:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    # error 
    # UnicodeEncodeError: 'cp949' codec can't encode character '\xe9' in position 1586: illegal multibyte sequence
    # UnicodeEncodeError: 'charmap' codec can't encode character '\u0302' in position 952: character maps to <undefined>
    
    # to fix the encode error,
    # use_unicode=False
    # x.decode("iso-8859-1").split('||')) \
    
    sc = SparkContext(appName="PythonTaxi")
    taxilines = sc.textFile(sys.argv[1], 1,  use_unicode=False)
    pointofInterest = sc.textFile(sys.argv[2], 1,  use_unicode=False)
    

    
    # 1. filter out
    # 2. get only Cell ID and day
    filteredTaxi = taxilines.map(lambda x: x.decode("iso-8859-1").split(',')) \
    .filter(correctFormat) \
    .map(lambda x: (getCellID(float(x[9]), float(x[8])), getDay(x[3]) ) ) \
  
    # map point of interest with location and name
    #1. map the input and get the cell ID
    #2. reduce function that add up the list of place if the location are the same
    #3. sort for fast lookup.
    placelist = pointofInterest.map(lambda x: x.decode("iso-8859-1").split('||')) \
    .filter(correctPoint) \
    .map(lambda x: (getCellID(float(x[0]), float(x[1])), x[2]) ) \
    .reduceByKey(lambda a,b : a + ', ' +b) \
    .sortByKey() \
    .collectAsMap()

    # get top 20 Sunday taxi
    sundayTaxi = filteredTaxi.filter(isSunday) \
    .map(lambda x: (x[0], 1)) \
    .reduceByKey(add) \
    .map(lambda x: (x[1], x[0])) \
    .top(20)
    
    # get top 20 week taxi
    # different way to filter
    weekTaxi = filteredTaxi.filter(lambda x: x[1] != 0) \
    .map(lambda x: (x[0], 1)) \
    .reduceByKey(add) \
    .map(lambda x: (x[1],x[0])) \
    .top(20)
    
    print('\n')
    
    print ('Sunday location top 20')
    #print (sundayTaxi)
    sundaySet = set()

    for taxilist in sundayTaxi:
        # make a tuple of 3 item. get the name of point of interest
        temptuple = (taxilist[1], taxilist[0], lookforplaces(taxilist[1]))
        # add to set
        sundaySet.add(temptuple)
    
    for top20 in sundaySet:
        print (top20)
    
    
    print()
    weekSet = set()
    print ('Week Location top 20')
    #print (weekTaxi)
    for taxilist in weekTaxi:
        temptuple = (taxilist[1], taxilist[0], lookforplaces(taxilist[1]))
        weekSet.add(temptuple)
        
    for top20 in weekSet:
        print (top20)

    

    
    