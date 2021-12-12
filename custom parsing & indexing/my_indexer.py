import csv
import re
import unidecode
#from nltk.corpus import stopwords
from math import log
import time


FILE_00 =  'output_wiki_13.csv'
FILE_01 = 'output_wiki_11.csv'
FILE_02 = 'output_wiki_conrad.csv'
FILE_03 = 'output_wiki_war_and_peace.csv'
FILE_04 = 'output_wiki_en_02.csv'
PATH = 'datasets/'
LOG_BASE = 10


# format elapsed time into h:mm:ss
def timeFormater(elapsedTime):
    hour = int(elapsedTime / (60 * 60))
    mins = int((elapsedTime % (60 * 60)) / 60)
    secs = elapsedTime % 60
    return "{}:{:>02}:{:>05.2f}".format(hour, mins, secs)


# removes stop words and most of nonalphanumeric characters
def removeStopWord(document, stopWords):
    document = unidecode.unidecode(document)
    document = document.lower()
    document = ' '.join([word for word in document.split() if word not in stopWords])
    #print(document)
    return document


# creates dictionary of terms and list of documents where they occur
def createTermDict(id, document, termDict):

    for index, word in enumerate(document.split()):
        if word.isalnum() and word not in termDict:
            termDict[word] = {}
            termDict[word]['docFreq'] = 1
            termDict[word][id] = {}
            termDict[word][id]['posList'] = []
            termDict[word][id]['posList'].append(index)
            termDict[word][id]['termFreq'] = len(termDict[word][id]['posList'])

        elif word.isalnum() and word in termDict:
            if id not in termDict[word]:
                termDict[word]['docFreq'] += 1
                termDict[word][id] = {}
                termDict[word][id]['posList'] = []
                termDict[word][id]['posList'].append(index)
                termDict[word][id]['termFreq'] = len(termDict[word][id]['posList'])

            elif id in termDict[word]:
                termDict[word][id]['posList'].append(index)
                termDict[word][id]['termFreq'] = len(termDict[word][id]['posList'])

    return termDict


# calculates tf-idf for every term
def calculateTfIdf(term, termDict, numOfDocs):

    docFreq = termDict['docFreq']
    idf = log(numOfDocs/docFreq, LOG_BASE)
    tfIdfDict = {}

    for key in termDict:
        if isinstance(termDict[key], int) == False:
            tf = termDict[key]['termFreq']
            tfIdf = tf * idf
            if tfIdf <= 0:
                tfIdf = 0

            if term not in tfIdfDict:
                tfIdfDict[term] = {}
                tfIdfDict[term][int(key)] = tfIdf
            elif term in tfIdfDict:
                tfIdfDict[term][int(key)] = tfIdf

    return tfIdfDict


# read found documents from file
def readFoundDocuments(offsets, docsToCheck):

    with open(PATH + FILE_04, 'r', encoding='utf-8') as file:

        for doc in docsToCheck:
            file.seek(offsets[doc[0]], 0)
            line = file.readline()
            formatOutput(line)


# format output in a naice way
def formatOutput(document):
    document = document.replace(',', '\n')
    document = document.replace(' br ', ' ')
    docList = document.split('\n')
    docList[2] = docList[2].replace('=', ':')
    type = re.findall(r'^[^\|]+ *\||$', docList[2])[0]
    docList[2] = re.sub('^[^\|]+ *\|', '', docList[2])
    docList[2] = docList[2].lstrip()
    finalDoc = docList[2].split('|')

    print('Document ID:', docList[0])
    print('Document name:', docList[1])
    print('Document type:', type.replace('|', ''))

    prev = ''
    newLine = ''
    for index, currItem in enumerate(finalDoc):
        if ':' in prev and ':' in currItem:
            newLine = '\n'

        if ':' in currItem:
            print(newLine,currItem, end='')
            prev = currItem
            newLine = ''
        else:
            print(newLine,currItem, end='')

    print('\n')


# main
if __name__ == "__main__":

    start = time.time()
    myStopWords = ['Infobox', 'infobox', 'getArgs', 'args', 'listclass', '|listclass', 'br',
                   'hlist', 'autocollapse', 'imagesize', 'caption', '|hlist', 'hlist|', 'listclass|']
    #stopWords = stopwords.words("english")
    #stopWords.extend(myList)

    termDict = {}
    tfIdfDict = {}
    fileOffsets = []
    numOfDocs = 0

    # vyratanie a ulozenie offsetov, nech vieme kde sa dokument zacina
    with open(PATH + FILE_04, 'rb') as file:
        while True:
            line = file.readline()
            if not line:
                break
            fileOffsets.append(file.tell())

    # indexovanie
    with open(PATH + FILE_04, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)

        for row in csv_reader:
            document = row['title'] + row['text']
            document = removeStopWord(document, myStopWords)
            termDict = createTermDict(row['id'], document, termDict)
            numOfDocs += 1

    for key in termDict:
        tfIdfDict.update(calculateTfIdf(key, termDict[key], numOfDocs))

    end = time.time()
    print('Indexing time: ', timeFormater(end - start))

    # vyhladavanie
    while True:
        query = input("Search for: ")
        query = query.lower()

        avgTfIdf = []
        found = []
        shared = []

        if query == 'q':
            break
        else:
            try:
                for term in query.split():
                    if term.isalnum() and term in tfIdfDict:
                        found.append(tfIdfDict[term])

                for item in found:

                    if len(shared) == 0:
                        shared = list(item.keys())

                    temp = list(item.keys())
                    shared = (list(set(shared).intersection(temp)))

                for doc in shared:
                    temp = 0
                    print(doc)
                    for item in found:
                        temp += item[doc]
                    avgTfIdf.append((doc, temp/len(shared)))

                avgTfIdf.sort(key=lambda x:x[1])
                readFoundDocuments(fileOffsets, avgTfIdf)

            except:
                print("Nothing relevant found ...")