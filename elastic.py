import json
import time
import re
from elasticsearch import Elasticsearch, helpers


IN_PATH = './datasets/'
WIKI_FILE = 'wiki_en_output.json'
TEST_FILE = 'sample_output.json'


LOCALHOST = 'http://localhost'
PORT_NUM = 9200

MAX_SIZE = 10000


# format elapsed time into h:mm:ss
def time_formater(elapsed_time):
    hour = int(elapsed_time / (60 * 60))
    mins = int((elapsed_time % (60 * 60)) / 60)
    secs = elapsed_time % 60
    return "{}:{:>02}:{:>05.2f}".format(hour, mins, secs)



# creates elastic index if needed
def create_index(elastic, index):
    res = elastic.indices.create(index=index, ignore=400)
    
    # make sure index was created
    if not res['acknowledged']:
        print('Error: Index was not created ...')
        print(res)
        exit(1)
    else:
        print('OK: Index was successfully created')



# process of indexing every document in csv file
def index_data(elastic, index):
    print('OK: Indexing data started')
    start = time.time()
    counter = 0
    indexed = 0

    with open(IN_PATH + WIKI_FILE, encoding='utf8') as file:
        
        for line in file:
            line = line.replace('[', '')
            line = line.replace(']', '')

            if line[len(line) - 2] == ',':
                line = line[:-2]

            document = json.loads(line)
            
            if '' in document:
                document['none'] = document['']
                del document['']
            
            try:
                elastic.index(index=index, id=document['id'], document=json.dumps(document))
                indexed += 1
            except Exception:
                counter += 1
                if counter % 1000 == 0:
                    print(counter, indexed)

    end = time.time()
    print('OK: Indexing data finished')
    print('Elapsed time:', time_formater(end - start))
    print('Indexed pages:', indexed)
    print('Failed to index:', counter, 'pages')
   


# process users query
def process_query(user_query, operator):

    query = { "multi_match": {
                "query": user_query,
                "operator": operator
            }
        }

    if ":" in user_query:
        query_list = user_query.split('\'')
        query_list = list(filter(None, query_list))

        while " " in query_list:
            query_list.remove(" ")
        
        fields = []
        values = []

        for index, item in enumerate(query_list):
            if ':' in item:
                field_name = query_list[index - 1]
                field_val = query_list[index + 1]
                fields.append(field_name)
                values.append(field_val)

        matching_list = []
        for index, item in enumerate(fields):
            if item == 'all':
                to_match = { "multi_match": { "query": values[index], "operator": operator }}
                matching_list.append(to_match)
            else:
                to_match = { "match": { item: {  "query": values[index], "operator": operator }}}
                matching_list.append(to_match)

        query = { "bool": { "must": matching_list } }

    return query



# search in indexed data
def search(elastic, index, user_query, operator):  
    query = process_query(user_query, operator)
    results = elastic.search(index=index, query=query, size=MAX_SIZE)
    return results



# process found documents and prints them in a nice way
def process_resluts(results):
    for res in results["hits"]["hits"]:
        document = res["_source"]

        for key in document:
            print(key, ": ", document[key])

        print("\n")



# main
if __name__ == "__main__":
    elastic = Elasticsearch(HOST=LOCALHOST, PORT=PORT_NUM)
    elastic = Elasticsearch()

    index = "wiki_index"

    # creation of index
    if not elastic.indices.exists(index=index):
        create_index(elastic, index)
        index_data(elastic, index)

    print("\n When searching:\n   h: help\n   q: quit\n\n Operator AND used defaultly")
    print("   query -OR: use OR operator\n")

    # searching stuff
    while True:
        query = input("Search for: ")

        if query == 'q':
            print("Exiting program ...")
            break
        
        if query == 'h':
            print("\n HOW TO USE ............................................................\n")
            print(" How to search in every field: just write anything you want to look for")
            print(" How to search in specific fields: \'field name\': \'value you look for\'")
            print("\n You can add \'all\': \'value\' for searching in all fields\n")
            print("\n END ...................................................................\n")

        elif query:
            print("..........................................................................\n")
            operator = "AND"
            if "-OR" in query:
                operator = "OR"
                query = query.replace(" -OR", "")

            results = search(elastic, index, query, operator)
            process_resluts(results)