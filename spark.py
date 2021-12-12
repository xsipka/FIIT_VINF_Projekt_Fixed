import time
import re
import unidecode

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, UserDefinedFunction
from pyspark.sql.types import StringType


WIKI_PATH = '/data/datasets/'
WIKI_FILE = 'output_wiki_en_02.csv'

IN_PATH = '/data/datasets/raw data/'
IN_FILE = 'en_wiki_complete.xml'

TEST_FILE = 'wiki_sample.xml'


# format elapsed time into h:mm:ss
def time_formater(elapsed_time):
    hour = int(elapsed_time / (60 * 60))
    mins = int((elapsed_time % (60 * 60)) / 60)
    secs = elapsed_time % 60
    return "{}:{:>02}:{:>05.2f}".format(hour, mins, secs)


# removes excess whitespace from string + other stuff
def string_formater(string, basic=True):

    if basic:
        string = unidecode.unidecode(string)
        string = string.lower()
        string = string.replace('noinclude', '')
        string = re.sub(r'[\"\[\]]', ' ', string)
        string = re.sub(r' +', ' ', string)
        string = re.sub(r'[^a-z0-9|()*=]+', ' ', string)
    else:
        string = re.sub(r'{.*?(?=\})',' ', string)
        string = re.sub(r'(?i)< *ref.*< */ *ref',' ', string)

    return string


# extract some informations about writer from writer pages
def extract_writer_page(page):

    try:
        page = page.replace('\n', ' ')
        data = re.search('(?i)== *(works|bibliography) *==.*?(?=(  *==[a-zA-Z ]*==))', str(page)).group()
    
        if data == None:
            return 'writer | '
        else:
            #data = data.group(0)
            data = string_formater(data, False)

        return 'writer | ' + data
        
    except Exception:
        pass

    return 'writer | '


# extract data out of infoboxes by using regular expressions
def extract_infobox_data(page):
    data = []

    # select from infobox book or short story
    if re.search('(?i){{Infobox *(book|short story)', page):
        to_save = 'book | '
        data.append(re.findall('(?i)name *=.*?(?=\|)\|', page))
        data.append(re.findall('(?i)author *=.*?(?=\])\] *\|', page))
        data.append(re.findall('(?i)genre *=.*?(?=\])\] *\|', page))
        data.append(re.findall('(?i)pages *=.*?(?=\|)\|', page))

        for item in data:
            to_save += ''.join(item)
        return to_save

    # select from infobox film
    elif re.search('(?i){{Infobox *film', page):
        to_save = 'film | '
        data.append(re.findall('(?i)name *=.*?(?=\|)\|', page))
        data.append(re.findall('(?i)director *=.*?(?=\])\] *\|', page))
        data.append(re.findall('(?i)based_on *=.*?(?=\}\})\}\} *\|', page))

        for item in data:
            to_save += ''.join(item)
        return to_save

    return page


# extract data out of infoboxes by using regular expressions
def extract_navbox_data(page):
    data = []
    to_save = 'navbox | '

    data.append(re.findall('(?i)name *=.*?(?=\|)\|', page))
    data.append(re.findall('(?i)group\d+ *=.*', page))

    for item in data:
        to_save += ''.join(item)
    to_save = re.sub('(?i){{noitalic\|\(\d+\)\}\}', ' ', to_save)
    return to_save


# parse Infoboxes and Navboxes using regular expressions
def select_boxed_data(content):
    buffer = []
    content_parser = False
    content_helper = False

    for line in content.splitlines():
        if re.search('(?i){{Infobox *(book|short story|film)', line) or re.search('(?i){{Navbox', line):
            content_parser = True
            buffer.append(line)
            continue

        if content_parser and re.search('{{', line):
            content_helper = True

        if content_helper and re.search('}}', line):
            content_helper = False
            buffer.append(line)
            continue

        if content_parser and content_helper == False and re.search('}}', line):
            content_parser = False
            content_helper = False
            buffer.append(line)
            continue

        if content_parser:
            buffer.append(line)
            continue

    to_return = ' '.join(buffer).replace('\n', ' ')
    return to_return


# save and extract informations from selected pages, throw away others
def save_page(content):

    # extract information from selected infoboxes
    if re.search('(?i){{Infobox *(book|short story|film)', str(content)):
        content = select_boxed_data(content)
        to_return = extract_infobox_data(content)
        to_return = string_formater(to_return)
        return to_return

    # extract information from selected navboxes
    elif re.search('(?i){{Navbox', str(content)):
        content = select_boxed_data(content)
        to_return = extract_navbox_data(content)
        to_return = string_formater(to_return)
        return to_return

    # extract information from pages about writers (their bibliography)
    elif re.search('(?i)== *(works|bibliography) *==', str(content)):
        to_return = extract_writer_page(content)
        to_return = string_formater(to_return)
        return to_return

    # return None if page not relevant
    return None


# main
if __name__ == "__main__":

    # run spark locally
    # spark = SparkSession.builder.appName('VINF Projekt')\
    #    .config('spark.jars', '/data/spark-xml_2.12-0.14.0.jar')\
    #    .config('spark.executor.extraClassPath', 'file://data/spark-xml_2.12-0.14.0.jar')\
    #    .config('spark.executor.extraLibrary', 'file://data/spark-xml_2.12-0.14.0.jar')\
    #    .config('spark.driver.extraClassPath', 'file://data/spark-xml_2.12-0.14.0.jar')\
    #    .getOrCreate()

    spark = SparkSession.builder.appName('VINF Projekt')\
        .master('spark://spark:7077')\
        .getOrCreate()


    root = 'mediawiki'
    row = 'page'

    schema = StructType([StructField('id', StringType(), True),
                        StructField('title', StringType(), True),
                        StructField('revision', StructType([StructField('text', StringType(), True)]))])

    df = spark.read.format('com.databricks.spark.xml')\
        .options(rootTag=root)\
        .options(rowTag=row)\
        .schema(schema)\
        .load(IN_PATH + IN_FILE)

    df = df.withColumn("revision", col("revision").cast("String"))
    df = df.withColumnRenamed("revision", "text")

    start = time.time()

    my_udf = UserDefinedFunction(save_page, StringType())

    df_new = df.withColumn('text', my_udf('text'))
    df_new = df_new.na.drop()

    try:
        df_new.repartition(10).write.format('com.databricks.spark.csv') \
            .save(WIKI_PATH + "spark_output", header = 'true')
        
    except Exception as e:
        print(e)
        exit(1)

    end = time.time()
    print('Elapsed time: ', time_formater(end - start))