import xml.sax
import csv
import os
import time
import re
import unidecode

FILE_00 =  'en_wiki_13.xml'
FILE_01 = 'en_wiki_11.xml-p6899367p7054859'
FILE_02 = 'wiki_sample.xml'
FILE_03 = 'war_and_peace_wiki.xml'
FILE_04 = 'en_wiki_complete.xml'
IN_PATH = 'datasets/raw data/'
OUT_PATH = 'datasets/'
MAX_LIST_LENGTH = 15000

pageId = 0

# format elapsed time into h:mm:ss
def timeFormater(elapsedTime):
    hour = int(elapsedTime / (60 * 60))
    mins = int((elapsedTime % (60 * 60)) / 60)
    secs = elapsedTime % 60
    return "{}:{:>02}:{:>05.2f}".format(hour, mins, secs)


# removes excess whitespace from string + other stuff
def stringFormater(string, basic=True):
    if basic:
        string = unidecode.unidecode(string)
        string = string.lower()
        string = re.sub(r'[\"\[\]]', ' ', string)
        string = re.sub(r' +', ' ', string)
        string = re.sub(r'[^a-z0-9|()*=]+', ' ', string)
    else:
        string = re.sub(r'{.*?(?=\})',' ', string)
        string = re.sub(r'(?i)< *ref.*< */ *ref',' ', string)

    return string


# class used for parsing XML file page by page
class PageHandler(xml.sax.handler.ContentHandler):
    def __init__(self):
        xml.sax.handler.ContentHandler.__init__(self)
        self.buffer = None
        self.values = {}
        self.element = None
        self.pageCounter = 0
        self.pageList = []
        self.contentParser = False
        self.contentHelper = False
        self.pageSaver = False
        self.writerPageSaver = False


    # add retrieved content into buffer
    def characters(self, content):
        if self.element == 'title':
            self.buffer.append(content)

        if self.element == 'text' and self.findUsefulData(content):
            self.buffer.append(content)

        elif self.element == 'text' and self.findWriterPage(content):
            self.buffer.append(content)


    # check if starting tag of element whose content we want to retrieve (title or text)
    def startElement(self, element, attrs):
        if element in ('title', 'text'):
            self.element = element
            self.buffer = []


    # check if ending tag of element, save retrieved content
    def endElement(self, element):
        if element == self.element:
            value = ' '.join(self.buffer).replace('\n', ' ')
            self.values[element] = value

        if element == 'page' and (self.pageSaver or self.writerPageSaver):
            self.pageList.append((self.values['title'], self.values['text']))
            self.pageCounter += 1
            self.pageSaver = False
            self.writerPageSaver = False

        if len(self.pageList) > MAX_LIST_LENGTH:
            self.saveListAsCSV()
            self.pageList = []
            print("25k hotovo :)")


    # find pages about writers
    def findWriterPage(self, content):
        # page is probably about some writer
        # (?i)== *[a-zA-Z]*(works|bibliography)[a-zA-Z]* *==
        # (?i)(novelist|writer|essayist|poet)
        # (?i)== *(works|bibliography) *==.*?(?=(  *==[a-zA-Z ]*==))
        if self.writerPageSaver:
            return True
        if re.search('(?i)== *(works|bibliography) *==', content):
            self.writerPageSaver = True
            return True
        return False


    # parse Infoboxes and Navboxes using regular expressions
    def findUsefulData(self, content):

        if re.search('(?i){{Infobox *(book|short story|film)', content) or re.search('(?i){{Navbox', content):
            self.contentParser = True
            self.pageSaver = True
            return True

        if self.contentParser and re.search('{{', content):
            self.contentHelper = True

        if self.contentHelper and re.search('}}', content):
            self.contentHelper = False
            return True

        if self.contentParser and self.contentHelper == False and re.search('}}', content):
            self.contentParser = False
            self.contentHelper = False
            return True

        if self.contentParser:
             return True

        return False


    # extrac useful data out of infoboxes & navboxes
    def processPage(self, page):

        if re.search('(?i){{Infobox *(book|short story|film)', page):
            data = self.extractInfoboxData(page)
            return data

        elif re.search('(?i){{Navbox', page):
            data = self.extractNavboxData(page)
            return data

        elif re.search('(?i)== *(works|bibliography) *==', page):
            # return page
            data = self.extractWriterPage(page)
            return data

        return page


    # extract some informations about writer from writer pages
    def extractWriterPage(self, page):
        # (?i)==[a-zA-Z ]*(works|bibliography)[a-zA-Z ]*==.*?(?=(  *==[a-zA-Z ]*==))
        # (?i)== *(works|bibliography) *==.*?(?=( *==[a-zA-Z ]*==))
        data = re.match('(?i)== *(works|bibliography) *==.*?(?=(  *==[a-zA-Z ]*==))', page)
        if data == None:
            return ''
        else:
            data = data.group(0)
            data = stringFormater(data, False)
        
        #f = open("sample.txt", "a",encoding='utf-8')
        #f.write(data)
        #f.write("\n\n..........................................................................\n\n")
        #f.close()

        return 'writer | ' + data


    # extract data out of infoboxes by using regular expressions
    def extractInfoboxData(self, page):
        data = []

        # select from infobox book or short story
        if re.search('(?i){{Infobox *(book|short story)', page):
            toSave = 'book | '
            data.append(re.findall('(?i)name *=.*?(?=\|)\|', page))
            data.append(re.findall('(?i)author *=.*?(?=\])\] *\|', page))
            data.append(re.findall('(?i)genre *=.*?(?=\])\] *\|', page))
            data.append(re.findall('(?i)pages *=.*?(?=\|)\|', page))

            for item in data:
                toSave += ''.join(item)
            return toSave

        # select from infobox writer
        elif re.search('(?i){{Infobox *writer', page):
            toSave = 'writer | '
            data.append(re.findall('(?i)[ *|\|]name *=.*?(?=\|)\|', page))
            data.append(re.findall('(?i)pseudonym *=.*?(?=\|)\|', page))

            for item in data:
                toSave += ''.join(item)
            return toSave

        # select from infobox film
        elif re.search('(?i){{Infobox *film', page):
            toSave = 'film | '
            data.append(re.findall('(?i)name *=.*?(?=\|)\|', page))
            data.append(re.findall('(?i)director *=.*?(?=\])\] *\|', page))
            #data.append(re.findall('(?i)based on *\|.*?(?=\}\})\}\} *\|', page))
            data.append(re.findall('(?i)based_on *=.*?(?=\}\})\}\} *\|', page))

            for item in data:
                toSave += ''.join(item)
            return toSave

        return page


    # extract data out of infoboxes by using regular expressions
    def extractNavboxData(self, page):

        data = []
        toSave = 'navbox | '

        data.append(re.findall('(?i)name *=.*?(?=\|)\|', page))
        data.append(re.findall('(?i)group\d+ *=.*', page))

        for item in data:
            toSave += ''.join(item)
        toSave = re.sub('(?i){{noitalic\|\(\d+\)\}\}', ' ', toSave)
        return toSave


    # save found data in file in CSV format
    def saveListAsCSV(self):
        header = ['id', 'title', 'text']
        global pageId

        with open(OUT_PATH + 'output.csv', 'a', encoding='utf-8') as file:
            writer = csv.writer(file)

            if os.path.getsize(OUT_PATH + 'output.csv') == 0:
                writer.writerow(header)

            for page in self.pageList:
                title = str(page[0])
                page = self.processPage(str(page[1]))
                page = stringFormater(page)
                row = [pageId, title, page]
                pageId += 1
                writer.writerow(row)


# main
if __name__ == "__main__":
    start = time.time()
    handler = PageHandler()
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    with open(IN_PATH + FILE_04, encoding='utf-8') as file:
        for line in file:
            parser.feed(line)

    if len(handler.pageList) > 0:
        handler.saveListAsCSV()

    end = time.time()
    print('Found pages: ', handler.pageCounter)
    print('Elapsed time: ', timeFormater(end - start))