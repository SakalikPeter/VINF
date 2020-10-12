from elasticsearch import Elasticsearch
import re

# files with entities types
typefiles = ['../data_train/yago-wd-full-types.nt', '../data_train/yago-wd-simple-types.nt']
# file with labeles
labelfile = '../data_train/yago-wd-labels.nt'

# dictionaries to save the data
all_entities = {}
all_labels = {}

def find_entity(line):
    """
    Parsing line from file to extract entity type and name
    input: <http://yago-knowledge.org/resource/Harald_Ringstorff>	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>	<http://yago-knowledge.org/resource/Human>	.
    output: Harald_Ringstorff, Human
    """
    parser = re.findall('/[^/]*>', line)

    entity_name = parser[0][1:-1]
    entity_type = parser[2][1:-1]

    return entity_name, entity_type


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


# reading files with entities types line by line
for filename in typefiles:
    with open(filename) as file:
        line = file.readline()

        while line:
            entity_name, entity_type = find_entity(line)
            insert_dict(all_entities, entity_type, entity_name)
            line = file.readline()


# reading file with labels
with open(labelfile) as file:
    line = file.readline()
    while line:
        entity, label = find_label(line)

        for key in all_entities:
            if entity in all_entities[key]:
                insert_dict(all_labels, key, label)
                break

        line = file.readline()


es = Elasticsearch()
num = 1 # index number

# indexing by ES
for key in all_labels:
    for item in all_labels[key]:
        res = es.index(index='entity_idx', id=num, body={key: item})
        num+=1
