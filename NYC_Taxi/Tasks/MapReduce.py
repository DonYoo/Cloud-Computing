from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
import datetime


#difference btw map and flatmap
# map : go each and change
# flatmap : make a new list

def getCellID(lat, lon):
    '''
    dividing all of the locations into grid cells by 3 decimal point.
    somehow it doesn't work in the class. i have to take out of the class as common function.
    '''
    return (str(round(lat, 3)) + " & "+str(round(lon, 3)))
    

class NYC_TAXI_TRIP:
    '''
    NYC taxi trip class that contains 3 different functions
    computing 
    1. taxi with most number of drivers: topTenTaxi()
    2. Comparing go-to place in NYC on Sundays morning 8 ~ 11am to other days
    3. Find some special events happened in the NYC
    '''
    def __init__(self, taxilines, pointofInterest):
        def correctFormat(listline):
            '''
            remove lines if they don't have 16 values
            also if values are empty.
            
            listline[0] medallion
            listline[1] driver ID is empty.
            listline[3] = dropoff_datetime
            listline[8] = dropoff_longitude
            listline[9] = dropoff_latitude
            '''
            if(len(listline) == 17):
                medallion = listline[0] 
                driverID = listline[1]
                time = listline[3]
                longi = float(listline[8].strip())
                lati  = float(listline[9].strip())
                
                if medallion and driverID and longi and lati and time: #is not empty and
                    if longi !=0.0 and lati != 0.0: #if value is not 0.
                            return listline

        def isfloat(value):
            '''
            check if the input is float
            '''
            try:
                float(value)
                return True
            except:
                return False           
            
        # 1. filter out taxi data set
        self.taxilines = taxilines.map(lambda x: x.decode("iso-8859-1").split(',')) \
        .filter(correctFormat)
        
        def correctPoint(listplace):
            '''
            filter out the places data set
            listplace[0] = latitude
            listplace[1] = longitude
            listplace[2] = name of POI
            '''
            lati  = listplace[0]
            longi = listplace[1]
            nameplace = listplace[2]
            # if lati and logi is float and nameofplace is not empty.
            if isfloat(lati) and isfloat(longi) and nameplace:
                return listplace

        '''
        map point of interest with location and name
        1. filter out point of interest data set
        2. map the input and get the cell ID
        3. reduce function that add up the list of place if the location are the same
        4. sort for fast lookup.
        '''        
        self.placelist = pointofInterest.map(lambda x: x.decode("iso-8859-1").split('||')) \
        .filter(correctPoint) \
        .map(lambda x: (getCellID(float(x[0]), float(x[1])), x[2]) ) \
        .reduceByKey(lambda a,b : a + ', ' +b) \
        .sortByKey() \
        .collectAsMap()            
   
    def lookforplaces(self, value):
            '''
            get the places within same grid cell position
            '''
            lookupv = self.placelist.get(value)
            if lookupv:
                return str(lookupv)
            return ''

    def topTenTaxi(self):
        """
        Top ten taxis that have had the largest number of drivers.
        taxi ID and Driver ID
        
        2. get only taxi ID and driver ID
        3. distinct items if taxi ID and driver ID are same
        4. after distinct, i don't need to care about driver ID anymore. 
            replace 1 with driver ID
        5. add up.
        6. swap key and value for sorting top
        """
        topTentaxi = self.taxilines \
        .map(lambda x: (x[0], x[1])) \
        .distinct() \
        .map(lambda x: (x[0], 1)) \
        .reduceByKey(add) \
        .top(10, lambda x:x[1])
        
        output = '1. Top ten taxis that have had the largest number of drivers.\n'
        output += 'TaxiID \t\t\t\t\t\t\t DriverID\n'
        
        # sort
        topTentaxi = sorted(topTentaxi, key = lambda x:x[1], reverse=True)
        for toplist in topTentaxi:
            output += str(toplist) + '\n'
        
        return output

    def compareSundayWeekMorning(self):
        '''
        top Twenty of Places
        where people in NYC go on Sundays mornings between (8am ~ 11am)
        comparing to other days of the week (Monday ~ Friday)
        
        return sundayTaxi, WeekTaxi
        '''
        def morningTime(value):
            '''
            # filter out file within 8 ~ 11 am
            '''
            date = value[3].split(' ')
            time = date[1].split(':')
            hour = int(time[0])
            if hour >= 8 and hour < 11:
                return True
            return False
        
        def getDay(value):
            '''
            daytime.
            weekday() Return the day of the week as an integer, 
            where Monday is 0 and Sunday is 6.
            '''
            dateform = value.split(' ')
            date = dateform[0].split('-')
            day = datetime.date(int(date[0]), int(date[1]), int(date[2]))
            return day.weekday()

        # 1. get the morning time
        # 2. get only Cell ID and day
        filteredTaxi = self.taxilines \
        .filter(morningTime) \
        .map(lambda x: (getCellID(float(x[9]), float(x[8])), getDay(x[3]))) \
        
        # value[1] is a day. 
        def isSunday(value):
            if value[1] == 0:
                return value

        # get top 20 Sunday taxi
        sundayTaxi = filteredTaxi.filter(isSunday) \
        .map(lambda x: (x[0], 1)) \
        .reduceByKey(add) \
        .top(20, lambda x:x[1])
        
        # get top 20 week taxi
        # different way to filter
        weekTaxi = filteredTaxi.filter(lambda x: x[1] != 0) \
        .map(lambda x: (x[0], 1)) \
        .reduceByKey(add) \
        .top(20, lambda x:x[1])
        
        sundaySet = set()
        for taxilist in sundayTaxi:
            # make a tuple of 3 item.
            # grid_cell, total_number of drop off, list of point of interest
            temptuple = (taxilist[0], taxilist[1], self.lookforplaces(taxilist[0]))
            # add to set
            sundaySet.add(temptuple)
        
        # sorted 
        sundaySet = sorted(sundaySet, key = lambda x:x[1], reverse=True)
        output = '\n2. Top Twenty of Places between 8 am ~ 11 am'
        output += '\nSunday location top 20\n'
        output += 'grid_cell \t total drop off \t list of (point of interest)\n'
        for top20 in sundaySet:
            output += str(top20) + '\n'
        
        weekSet = set()
        for taxilist in weekTaxi:
            temptuple = (taxilist[0], taxilist[1], self.lookforplaces(taxilist[0]))
            weekSet.add(temptuple)
        
        weekSet = sorted(weekSet, key = lambda x:x[1], reverse=True)
        output += '\nWeek Location top 20\n'
        output += 'grid_cell \t total drop off \t list of (point of interest)\n'
        for top20 in weekSet:
            output += str(top20) + '\n'
        
        return output

    def specialEvents(self):
        """
        grid-cell+hour of drop-offs
        compute every hour average of everyday for each grid 
        since a day is 24 hours, average of 24 different hours
        key has to be time and grid cell.
        """
        def setDate(value):
            '''
            get the date ex) '2013-01-01'
            '''
            dateform = value.split(' ')
            return dateform[0]

        def setHour(value):
            '''
            get the hour
            '''
            dateform = value.split(' ')
            # round to hour
            return (dateform[1].split(':'))[0]
        
        """
        Compute drop-off with reduce by same date and same hours
        make a format ((grid-cell+date+hour), drop-off)
        x[3] = date and hours
        """
        dateHourTaxi = self.taxilines \
        .map(lambda x: ((getCellID(float(x[9]), float(x[8])), setDate(x[3]), setHour(x[3]) ), 1 ) ) \
        .reduceByKey(add) \
        
        def getgrid(value):
            return value[0][0]

        def getTime(value):
            return value[0][2]
        
        # get how many dates on same place same time.
        # this is for compute average
        # ((grid, time), dates)
        totalDateTaxi = dateHourTaxi.map(lambda x: ((getgrid(x), getTime(x)), 1) ) \
        .reduceByKey(add) \
        .collectAsMap()
        
        def lookforDate(value):
            '''
            get the value from HashMap number of total dates.
            '''
            numberofdrop = totalDateTaxi.get(value)
            if numberofdrop:
                return numberofdrop
            return 1
    
        # total-drop-off / total dates = average
        averageTaxi = dateHourTaxi.map(lambda x: ((getgrid(x), getTime(x)), x[1])) \
        .reduceByKey(add) \
        .map(lambda x: (x[0], x[1]/lookforDate(x[0])) ) \
        .collectAsMap()
        
        
        def lookforAverage(value):
            '''
            get total number of drop in same grid and time
            '''
            lookupv = (getgrid(value), getTime(value))
            numberofdrop = averageTaxi.get(lookupv)
            if numberofdrop:
                return numberofdrop
            return 0
        
        # ratio = drop-off / average
        # get top 20
        top20 = dateHourTaxi \
        .map(lambda x: ((x[0][0], x[0][1], x[0][2], x[1]), x[1]/lookforAverage(x) )) \
        .map(lambda x: (x[1], x[0])) \
        .top(20)
        
        # make a set
        top20set = set()
        for eachitem in top20:
            # make a tuple of 6 item. get the name of point of interest
            temptuple = (eachitem[1][0], eachitem[1][2], eachitem[1][1], eachitem[0], \
                         eachitem[1][3], self.lookforplaces(eachitem[1][0]))
            # add to set
            top20set.add(temptuple)
        
        output = '\n3. Top ten special events in NYC .\n'
        output += 'grid_cell \t\t\t hours \t date \t\t ratio \t\t\t number drop \t list of (point of interest)\n'
        #sorted
        top20set = sorted(top20set, key=lambda x:x[3], reverse=True)
        for eachtop in top20set:
            output += str(eachtop) +'\n'
        
        return output
                
if __name__ == "__main__":
    
    # check if there is an argument for the input files
    if len(sys.argv) != 3:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    # open up the file as spark context
    sc = SparkContext(appName="PythonTaxi")
    
    # error in windows when it gets the weird character.
    # UnicodeEncodeError: 'cp949' codec can't encode character '\xe9' in position 1586: illegal multibyte sequence
    # UnicodeEncodeError: 'charmap' codec can't encode character '\u0302' in position 952: character maps to <undefined>
    
    # to fix the encode error,
    # use_unicode=False
    # x.decode("iso-8859-1").split('||')) \
    taxilines = sc.textFile(sys.argv[1], 1,  use_unicode=False)
    pointofInterest = sc.textFile(sys.argv[2], 1,  use_unicode=False)
    
    # create output file that combine all the outputs
    f = open('output.txt', 'w')
    
    # Taxi object set up the inputs and filter out
    nyc_taxi = NYC_TAXI_TRIP(taxilines, pointofInterest)
    
    topTenTaxi = nyc_taxi.topTenTaxi()
    f.write (topTenTaxi)
    
    compare = nyc_taxi.compareSundayWeekMorning()
    f.write(compare)
    
    average = nyc_taxi.specialEvents()
    f.write(average)
    
    f.close()