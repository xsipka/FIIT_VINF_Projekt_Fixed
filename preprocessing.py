import csv
import re
import sys
import json

IN_PATH = './datasets/'
WIKI_FILE = 'spark_output/wiki_complete.csv'
TEST_FILE = 'output_wiki_13.csv'



if __name__ == "__main__":

    counter = 0
    max_int = sys.maxsize

    while True:
        try:
            csv.field_size_limit(max_int)
            break
        except OverflowError:
            max_int = int(max_int/10)


    with open(IN_PATH + 'wiki_output.json', 'a') as output:
        output.write('[')

    with open(IN_PATH + WIKI_FILE, encoding='utf8') as file:

        reader = csv.DictReader(file)

        try:
            for row in reader:
                doc_id = row['id']
                title = row['title'] 
                text = row['text']
                text = text.replace('|', ' | ')
                text = text.replace('=', ' = ')
                text_list = text.split()
                doc_type = text_list[0]
                
                document = {"id": doc_id,
                            "title": title,
                            "type": doc_type}
                
                text_list = text.split('|')
                #print(text_list)
                fields_list = []
                temp =[]
                start = False
                list_length = len(text_list) - 1

                for index, item in enumerate(text_list):
                    if index == 0:
                        continue
                    
                    if index == list_length and temp:
                        fields_list.append(temp)

                    if start and '=' not in item:
                        temp.append(item)

                    if start and '=' in item:
                        fields_list.append(temp)
                        start = False
                        temp = []

                    if '=' in item:
                        start = True
                        temp.append(item)

                to_process = []
                for field in fields_list:
                    temp = []
                    for elem in field:
                        curr = elem.split('=')
                        for item in curr:
                            temp.append(item)
                    to_process.append(temp)

                to_save = []
                for field in to_process:
                    temp = []
                    for item in field:
                        item = re.sub(' +', ' ', item)
                        item = item.lstrip()
                        item = item.rstrip()
                        temp.append(item)
                    to_save.append(temp)


                for field in to_save:
                    field_val = []
                    field_name = ''
                    for index, item in enumerate(field):
                        if index == 0:
                            field_name = item
                        else:
                            if item == '':
                                continue
                            field_val.append(item)
                    value = ', '.join(field_val)
                    to_update = {field_name: value}
                    document.update(to_update)
                    #print(field_name, ":",  value)


                with open(IN_PATH + 'wiki_output.json', 'a') as output:
                    counter += 1
                    json.dump(document, output)
                    output.write(',\n')

                if counter % 50000 == 0:
                    print('50k documents saved :)')
        
        except Exception as e:
            print(e)
        

    with open(IN_PATH + 'wiki_output.json', 'a') as output:
        test = {"id": 0, "title": "test", "type": "test"}
        json.dump(test, output)
        output.write(']')