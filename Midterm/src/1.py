import sys
from operator import add
from pyspark import SparkContext
from pyspark.shuffle import GroupByKey
from networkx.algorithms.operators.binary import intersection

"""
1. 
The option 2 using oop style over compute something simple or same pattern like csv is not necessary.
this option ends up creating many class that has very simple functions.

The option 1 is using RDD as array of string. however it does not care about imposing a schema, such as columnar format, 
while processing or accessing data attributes by name or column;

The option 3 using dataframe would be the best choice since DataFrame has better performance and scalability.
it has concept of a schema to describe the data.
on large datasets, the DataFrame API is indeed very easy to use and also faster.
"""
    
# remove lines if they don't have 7 values
def productFormat(listline):
    if(len(listline) == 7):
        try:
            productID = listline[0]
            price  = float(listline[2])
            weight = float(listline[3])
            supplierID = listline[6]
            if productID and price and supplierID: #is not empty and
                return listline
        except:
            print('not valid input {} {} {}'.format(listline[0], listline[2], listline[6]))
            return 
        
# remove lines if they don't have 5 values
def orderFormat(listline):
    if(len(listline) == 5):
        try:
            volume = int(listline[3])
            price  = float(listline[4])
            if volume and price: #is not empty and
                return listline
        except:
            print('not valid input {} {}'.format(listline[3], listline[4]))
            return 

# remove lines if they don't have 5 values
def customerFormat(listline):
    if(len(listline) == 5):
        try:
            for i in range(5):
                if not listline[i]:
                    return
            return listline
        except:
            print('not valid input {} {} {} {} {}' \
                  .format(listline[0], listline[1], \
                          listline[2], listline[3], \
                          listline[4]))
            return 

def num2(orderfile):
    """ 
    2. top 10 sold products.
        1. filter out
        2. get only product ID and volume    
        3. add up.
        4. get top 10 by sort value
    """
    toptenSoldProducts = orderfile.map(lambda x: x.split(',')) \
    .filter(orderFormat) \
    .map(lambda x: (x[2], int(x[3]))) \
    .reduceByKey(add) \
    .top(10, key = lambda x: x[1])

    for i in toptenSoldProducts:
        print (i)
    return toptenSoldProducts
    
def num3(orderfile):
    """ 
    3. top 10 customers.
        1. filter out
        2. get only customer ID and volume    
        3. add up.
        4. get top 10 by key of value
    """
    toptenSoldCustomer = orderfile.map(lambda x: x.split(',')) \
    .filter(orderFormat) \
    .map(lambda x: (x[1], int(x[3]))) \
    .reduceByKey(add) \
    .top(10, key = lambda x: x[1])

    for i in toptenSoldCustomer:
        print (i)
    return toptenSoldCustomer


def num4(productfile, orderfile):
    """ 
    4. (product, supplier)
        1. filter out
        2. get only product ID and supplier ID
        3. if product has same supplier, reduce them to one.(distinct)
    """
    productSupplier = productfile.map(lambda x: x.split(',')) \
    .filter(productFormat) \
    .map(lambda x: (x[0], x[6])) \
    .distinct()

    
    """
    get cust+supplier as a key, int(volume) as a value
    """
    def getCustomerSupplier(inputlist):
        cust = (inputlist[1][0][0])
        supplier = (inputlist[1][1])
        volume = int(inputlist[1][0][1])
        
        return ((cust, supplier), volume)

    """ 
    4. (product, (customer, volume)
        1. filter out
        2. get only product ID and (customer ID, volume)
        3. join with product supplier => product, (customer ID, volume, supplier)
        4. map as (cust&supplier, volume)
        5. reducebyKey
        6. get top 10
    """
    toptenSameSupplierCustomer = orderfile.map(lambda x: x.split(',')) \
    .filter(orderFormat) \
    .map(lambda x: (x[2], (x[1], x[3]) )) \
    .join(productSupplier) \
    .map(lambda x: (getCustomerSupplier(x)) ) \
    .reduceByKey(add) \
    .top(10, key = lambda x: x[1])
    
    for i in toptenSameSupplierCustomer:
        print (i)
    return toptenSameSupplierCustomer

def num5(customerfile, orderfile, productfile):
    
    # get (customer, product)
    customerProduct = orderfile.map(lambda x:x.split(',')) \
    .filter(orderFormat) \
    .map(lambda x: (x[1], x[2]))
    
    # get (customer, country)
    customerCountry = customerfile.map(lambda x: x.split(',')) \
    .filter(customerFormat) \
    .map(lambda x: (x[0], x[3]))
    
    # get (product, country)
    productCountry = productfile.map(lambda x:x.split(',')) \
    .filter(productFormat) \
    .map(lambda x: (x[0], x[4]))
    
    
    """
        1. compare customer country with
        2. iterate through with prod country
        3. if they are match, don't return.
    """
    def removeSamecountry(listline):
        custCountry = listline[0][1]
        prodCountry = listline[1]
#        print('cust con: ', custCountry)
        for index in prodCountry:
 #           print('prod con: ', index)
            if custCountry == index:
                return
        return listline

    """
    5. (customerID, country)
        1. join and make RDD of (cust, (prod, country))
        2. distinct to remove if they order same product
        3. change to (prod, (cust, country)) 
        4. join product, country
        = (prod, (cust, country) prod_country) 
        5. drop the product ID and make key of cust, country and
        tuple of value!!
        = ((cust, country), (prod_country,))
        6. distinct and drop the different prod but same prod_coutnry
        7. reduceByKey add up the list
        8. filter out
    """    
    custProdCount = customerProduct.join(customerCountry) \
    .distinct() \
    .map(lambda x: (x[1][0],(x[0],x[1][1])) ) \
    .join(productCountry) \
    .map(lambda x: ((x[1][0][0], x[1][0][1]), (x[1][1],) )) \
    .distinct() \
    .reduceByKey(add) \
    .filter(removeSamecountry) \
    .collect()
    
    for i in custProdCount:
        print (i)
    return custProdCount

    
def num7(orderfile):

    """
    7. (customerID, country)
    1. get customer ID and productID
    2. reduce them as (custID, tuple(productID))
    3. distinct same product
    """
    setupCustomer = orderfile.map(lambda x: x.split(',')) \
    .filter(orderFormat) \
    .map(lambda x: (x[1],(x[2], ))) \
    .distinct() \
    .reduceByKey(add)
    
    """
    1. compare customer ID and put in order
    
    """
    def sortinOrder(listline):
        firstKey = listline[0][0]
        secondKey = listline[1][0]
        firstValue = listline[0][1]
        secondValue = listline[1][1]
        
        #
        if firstKey > secondKey:
            return ((secondKey,secondValue),(firstKey,firstValue))
        return listline
    
    """
    calculate Jaccard similarity
    get intersection and union
    """
    def calculateJaccard(listline):
        firstKey = listline[0][0]
        secondKey = listline[1][0]
        firstValue = listline[0][1]
        secondValue = listline[1][1]
        
        firstsecondKey = (firstKey, secondKey)
        
        intersection = 0.0
        for index in firstValue:
            if index in secondValue:
                intersection += 1
        union = len(firstValue)+len(secondValue) - intersection
        
        similarity = intersection / union
        
        return (firstsecondKey, similarity)
        
    """
    7. (customerID, country)
    1. get cartesian with same rdd
    2. filter out if key and values are same.
    3. sort the 2 inputs in order
    4. drop the same input
    5. calculate jaccard similarity
    6. get top 10 by the key
    
    """
    similarCustomer = setupCustomer \
    .cartesian(setupCustomer) \
    .filter(lambda x:x[0]!=x[1]) \
    .map(lambda x: sortinOrder(x)) \
    .distinct() \
    .map(lambda x:calculateJaccard(x)) \
    .top(10, key = lambda x: x[1])
    
    for i in similarCustomer:
        print (i)
    
    return

def num8(orderfile):
    
    """
    iterate through and make all possible (pairs, 1)
    """
    def makePairs(listline):
        listofPairs =[]
        
        if len(listline) > 1:
            for i in range(len(listline)-1):
                for j in range(i+1,len(listline)):
                    listofPairs.append( ((listline[i], listline[j]),1) )
        return listofPairs

        
        
    """
    8. (customerID, country)
        1. get customer ID and productID
        2. reduce them as (custID, tuple(productID))
        3. distinct same product
        4. sort the values in order
        5. reduce to a list (custID, tuple(productID))
        6. drop the cust ID. don't need it
        7. make a new list of pairs of all possible pairs
        = ((prod1, prod2), 1)
        8. reduce and add up the list
        9. get top 10 with value
    """
    
    toptenproductPair = orderfile.map(lambda x: x.split(',')) \
    .filter(orderFormat) \
    .map(lambda x: (x[1],(x[2], ))) \
    .distinct() \
    .sortBy(lambda x:x[1]) \
    .reduceByKey(add) \
    .map(lambda x: x[1]) \
    .flatMap(lambda x: makePairs(x)) \
    .reduceByKey(add) \
    .top(10, key = lambda x:x[1])

    for i in toptenproductPair:
        print (i)
    return toptenproductPair   



if __name__ == "__main__":
    
    if len(sys.argv) != 4:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Midterm")
    
    customerfile = sc.textFile(sys.argv[1], 1)
    orderfile = sc.textFile(sys.argv[2], 1)
    productfile = sc.textFile(sys.argv[3], 1)
    
    
    print()
    print()
    
    print('number 2 toptenSoldProducts')
    toptenSoldProducts = num2(orderfile)
    print('number 3 toptenSoldCustomer')
    toptenSoldCustomer = num3(orderfile)
    print('number 4 toptenSameSupplierCustomer')
    toptenSameSupplierCustomer = num4(productfile, orderfile)
    print('number 5 toptenForeignProduct')
    toptenForeignProduct = num5(customerfile, orderfile, productfile)
    print('number 7 similarCustomer')
    similarCustomer = num7(orderfile)
    print('number 8 toptenproductPair')
    toptenProductPair = num8(orderfile)
 
    