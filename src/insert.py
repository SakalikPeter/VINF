from elasticsearch import Elasticsearch
from elasticsearch import helpers
import re


def find_entity(line):
    """
    Parsing line from file to extract entity type and name
    input: <http://yago-knowledge.org/resource/Harald_Ringstorff>	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>	<http://yago-knowledge.org/resource/Human>	.
    output: Harald_Ringstorff, Human
    """
    parser = re.findall('/[^/]*>', line)

    entity_name = parser[0][1:-1]
    entity_type = parser[2][1:-1]

    return entity_type, entity_name


def find_label(line):
    """
    Parsing line from file to extract entity name and label
    input: <http://yago-knowledge.org/resource/Harald_Ringstorff>	<http://www.w3.org/2000/01/rdf-schema#label>	"Harald Ringsdörp"@nds	.
    ouptu: Harald_Ringstorff, Harald Ringsdörp
    """
    entity_parser = re.findall("/[^/]*>", line)
    label_parser = re.findall("\".*\"", line)

    entity = entity_parser[0][1:-1]
    label = label_parser[0][1:-1]

    return entity, label


def insert_dict(dict, key, value):
    """
    Inserting values to dictionary
    """
    try:
        dict[key].add(value)
    except KeyError:
        dict[key] = {value}


def main():
    directory = '../data/'
    # files with entities types
    typefiles = ['yago-wd-full-types.nt', 'yago-wd-simple-types.nt']
    # file with labeles
    labelfiles = ['yago-wd-labels.nt']

    # dictionaries to save the data
    all_types = {}
    all_entities = {}

    # reading files with entities types line by line
    for filename in typefiles:
        with open(f"{directory}{filename}") as file:
            line = file.readline()

            while line:
                entity_type, entity_name = find_entity(line)
                # save types for searching labels
                insert_dict(all_types, entity_type, entity_name)
                # save entities for indexing
                insert_dict(all_entities, entity_type.replace('_', ' '), entity_name.replace('_', ' '))
                line = file.readline()


    # reading files with labels line by line
    for filename in labelfiles:
        with open(f"{directory}{filename}") as file:
            line = file.readline()

            while line:
                entity_type, entity_name = find_label(line)

                for key in all_types:
                    if entity_type in all_types[key]:
                        # save entities for indexing
                        insert_dict(all_entities, key.replace('_', ' '), entity_name)
                        break

                line = file.readline()


    es = Elasticsearch()
    num = 1 # index number

    # indexing by ES
    for key in all_entities:
        actions = []
        print(f"indexing entity {key}")

        for item in all_entities[key]:
            action = {
                "_index": "entity_index",
                "_id": num,
                "_source": {
                    'label': key,
                    'pattern': item
                }
            }
        
            num += 1 
            actions.append(action)

            # index after 0,5M records
            if not num % 500000:
                helpers.bulk(es, actions)
                actions = []
        
        # index batch
        helpers.bulk(es, actions)


if __name__ == "__main__":
  main()