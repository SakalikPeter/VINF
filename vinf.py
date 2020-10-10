import re

typefiles = ['data_short/yago-wd-full-types.nt', 'data_short/yago-wd-simple-types.nt']
labelfile = 'data_short/yago-wd-labels.nt'

all_entities = {}
all_labels = {}

def find_entity(line):
    parser = re.findall('/[^/]*>', line)
    entity_name = parser[0][1:-1]
    entity_type = parser[2][1:-1]
    return entity_name, entity_type


def find_label(line):
    entity_parser = re.findall("/[^/]*>", line)
    label_parser = re.findall("\".*\"", line)

    entity = entity_parser[0][1:-1]
    label = label_parser[0][1:-1]
    return entity, label


for filename in typefiles:
    with open(filename) as file:
        line = file.readline()

        while line:
            # print(line)
            entity_name, entity_type = find_entity(line)
            # print(entity_name, entity_type, '\n')
            try:
                all_entities[entity_type].add(entity_name)
            except KeyError:
                all_entities[entity_type] = {entity_name}
            
            line = file.readline()


# print(all_entities)
# print('\n')

with open(labelfile) as file:
    line = file.readline()
    while line:
        entity, label = find_label(line)

        for key in all_entities:
            if entity in all_entities[key]:
                try:
                    all_labels[key].add(label)
                except KeyError:
                    all_labels[key] = {label}
                break

        line = file.readline()


# print(all_labels['Human'])

# --- vyskusanie ES ---
from elasticsearch import Elasticsearch
es = Elasticsearch('http://localhost:9200')

print(type( all_labels['Human']))

res = es.index(index='human_idx', id=1, body={'Human': list(all_labels['Human'])})

res =  es.get(index='human_idx', id=1)
print('\n')
print(res)