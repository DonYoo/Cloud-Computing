import re
import numpy as np
from operator import add
from pyspark import SparkContext

# KNN = k nearest neighbors classifier
sc = SparkContext(appName="kNN")

"""
Task 1
"""
# First load up all of the 19997 documents in the corpus
#corpus = sc.textFile ("s3://metcs755/20_news_same_line.txt")

corpus = sc.textFile("20_news_same_line_real.txt")

# Each entry in validLines will be a line from the text file
validLines = corpus.filter(lambda x : 'id' in x)

# Now, we transform it into a set of (docID, text) pairs
keyAndText = validLines.map( lambda x : (x[x.index('id="') + 4 : x.index('" url=')], x[x.index('">') + 2:]))

#print(keyAndText.collect())

# Now, we split the text in each (docID, text) pair into a list of words
# After this step, we have a data set with 
# (docID, ["word1", "word2", "word3", ...])
# We use a regular expression here to make 
# sure that the program does not break down on some of the documents

regex = re.compile('[^a-zA-Z]')

keyAndListOfWords = keyAndText.map(lambda x : (str(x[0]), regex.sub(' ', x[1]).lower().split()))
#print (keyAndListOfWords.collect())


# Now get the top 20,000 words... 
# first change (docID, ["word1", "word2", "word3", ...])
# to ("word1", 1) ("word2", 1)...

allWords = keyAndListOfWords.map(lambda x: x[1]) \
.flatMap(lambda xs: [(x, 1) for x in xs])
#print (allWords.collect())

# Now, count all of the words, giving us ("word1", 1433), ("word2", 3423423), etc.
allCounts = allWords.reduceByKey(add)
#print (allCounts.collect())

# Get the top 20,000 words in a local array in a sorted format based on frequency 
topWords = allCounts.top(20000, key = lambda x: x[1])
#print(topWords) 

# We'll create a RDD that has a set of (word, dictNum) pairs
# start by creating an RDD that has the number 0 through 20000
# 20000 is the number of words that will be in our dictionary
twentyK = sc.parallelize(range(20000))

# Now, we transform (0), (1), (2), ... to ("mostcommonword", 1) 
# ("nextmostcommon", 2), ...
# the number will be the spot in the dictionary used to tell us 
# where the word is located
dictionary = twentyK.map (lambda x : (topWords[x][0], x))
#print (dictionary.collect())

# Next step, we get a RDD that has, for each 
# (docID, ["word1", "word2", "word3", ...]),
# ("word1", docID), ("word2", docId), ...
allWords = keyAndListOfWords.flatMap(lambda x: ((j, x[0]) for j in x[1]))
#print(allWords.collect())

# Now join and link them, to get a set of ("word1", (dictionaryPos, docID)) pairs
allDictionaryWords = dictionary.join (allWords)
#print(allDictionaryWords.collect())

# Now, we drop the actual word itself to get a set of (docID, dictionaryPos) pairs
justDocAndPos = allDictionaryWords.map (lambda x: (x[1][1], [x[1][0]]))
#print(justDocAndPos.collect())

# Now get a set of (docID, [dictionaryPos1, dictionaryPos2, dictionaryPos3...]) pairs
allDictionaryWordsInEachDoc = justDocAndPos.reduceByKey(add)
#print(allDictionaryWordsInEachDoc.collect())


"""
Task 2
"""
# Now, extract the newsgroupID, so that on input we have a set of
# (docID, [dictionaryPos1, dictionaryPos2, dictionaryPos3...]) pairs, 
# but on output we 
# have a set of ((docID, newsgroupID) [dictionaryPos1, dictionaryPos2, dictionaryPos3...]) pairs
# The newsgroupID is the name of the newsgroup extracted from 
# the docID... for example 
# if the docID is "20_newsgroups/comp.graphics/37261" then 
# the newsgroupID will be "/comp.graphics/"

regex = re.compile('/.*?/')
allDictionaryWordsInEachDocWithNewsgroup = allDictionaryWordsInEachDoc.map (lambda x: ((x[0], regex.search(x[0]).group (0)), x[1]))
#print(allDictionaryWordsInEachDocWithNewsgroup.collect())


# The following function gets a list of dictionaryPos values, 
# and then creates a TF vector
# corresponding to those values... for example, 
# if we get [3, 4, 1, 1, 2] we would in the
# end have [0, 2/5, 1/5, 1/5, 1/5] because 0 appears zero times, 
# 1 appears twice, 2 appears once, etc.

def buildArray (listOfIndices):
        returnVal = np.zeros (20000)
        for index in listOfIndices:
                returnVal[index] = returnVal[index] + 1
        
        mysum = np.sum (returnVal)
        
        returnVal = np.divide (returnVal, mysum)
        return returnVal


# The following line this gets us a set of 
# ((docID, newsgroupID) [dictionaryPos1, dictionaryPos2, dictionaryPos3...]) pairs
# and converts the dictionary positions to a bag-of-words numpy array... 

allDocsAsNumpyArrays = allDictionaryWordsInEachDocWithNewsgroup.map (lambda x: (x[0], buildArray (x[1])))
#print(allDocsAsNumpyArrays.collect())


# Now, create a version of allDocsAsNumpyArrays where, in the array, 
# every entry is either zero or one.  
# A zero means that the word does not occur,
# and a one means that it does.

def zeroOrOneCheck(s):
    for i in range(len(s)):
        if s[i] != 0:
           s[i] = 1  
    return s

zeroOrOne = allDocsAsNumpyArrays.map(lambda x: (x[0], zeroOrOneCheck(x[1])))
#print ( zeroOrOne.collect() )

# Now, add up all of those arrays into a single array, where the 
# i^th entry tells us how many
# individual documents the i^th word in the dictionary appeared in
dfArray = zeroOrOne.reduce (lambda x1, x2:(("", np.add (x1[1], x2[1]))))[1]
#print(dfArray.tolist())

# Create an array of 20,000 entries, each entry with the value 19997.0 (number of docs)
multiplier = np.full (20000, 19997.0)


# Get the version of dfArray where the i^th entry is the inverse-document frequency for the
# i^th word in the corpus
idfArray = np.log (np.divide (multiplier, dfArray))
#print(idfArray.tolist())

# Finally, convert all of the tf vectors in allDocsAsNumpyArrays to tf * idf vectors
allDocsAsNumpyArrays = allDocsAsNumpyArrays.map (lambda x: (x[0], np.multiply (x[1], idfArray)))
#print (allDocsAsNumpyArrays.collect())

"""
Task 3
"""


# Finally, we have a function that returns the prediction for the label of a string, using a kNN algorithm
def getPrediction (textInput, k):
    
    #print(dictionary. collect())
        
    # Create an RDD out of the textIput
    myDoc = sc.parallelize (('', textInput))
    # Flat map the text to (word, 1) pair for each word in the doc
    wordsInThatDoc = myDoc.flatMap (lambda x : ((j, 1) for j in regex.sub(' ', x).lower().split()))
#   print (wordsInThatDoc.collect())
    
    
    #
    # This will give us a set of (word, (dictionaryPos, 1)) pairs
    # convert it to (1, dictionaryPos) pairs
    # add up and make it as (1, (dictionaryPos1, dictionaryPos2 ...))
    allDictionaryWordsInThatDoc = dictionary.join (wordsInThatDoc).map(lambda x: (x[1][1], x[1][0])).groupByKey()
#   print(allDictionaryWordsInThatDoc.collect())


    # Get tf array for the input string
    myArray = buildArray (allDictionaryWordsInThatDoc.top (1)[0][1])

    #
    # Get the tf * idf array for the input string
    myArray = np.multiply (myArray, idfArray)
    #print(myArray.tolist())

    #
    # Get the distance from the input text string to all database documents, using cosine similarity
    # multiply tf * idf of input and list then sum them up.
    distances = allDocsAsNumpyArrays.map (lambda x: (x[0][1], np.sum(np.multiply(x[1], myArray)) ))
    #print('distance', distances.collect())
    
    
    #
    # get the top k distances
    topK = distances.top (k, lambda x : x[1])
    #print ('topK' , topK)
    
    
    #
    # and transform the top k distances into a set of (newsgroupID, 1) pairs
    newsgroupsRepresented = sc.parallelize (topK).map (lambda x : (x[0], 1))
    #print ('news' , newsgroupsRepresented.collect())
    
    #
    # now, for each newsgroupID, get the count of the number of times this newsgroup appeared in the top k
    numTimes = newsgroupsRepresented.aggregateByKey (0, lambda x1, x2: x1 + x2, lambda x1, x2: x1 + x2)
    #print ('times' , numTimes.collect())
    
    # Return the top 1 of them.  
    return numTimes.top (1, lambda x: x[1])

if __name__ == "__main__":
    print(getPrediction('How many goals Vancouver score last year?', 30))