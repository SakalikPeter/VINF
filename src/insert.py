from elasticsearch import Elasticsearch
from elasticsearch import helpers
import re


def find_entity(line):
    """
    Parsing line from file to extract entity type and name
    input:  line: <http://yago-knowledge.org/resource/Harald_Ringstorff>	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>	<http://yago-knowledge.org/resource/Human>	.
    output: entity_type: Human
            entity_name: Harald_Ringstorff
    """
    parser = re.findall('/[^/]*>', line)

    entity_name = parser[0][1:-1]
    entity_type = parser[2][1:-1]

    return entity_type, entity_name


def find_label(line):
    """
    Parsing line from file to extract entity name and label
    input:  line: <http://yago-knowledge.org/resource/Harald_Ringstorff>	<http://www.w3.org/2000/01/rdf-schema#label>	"Harald Ringsdörp"@nds	.
    ouptu:  entity: Harald_Ringstorff
            label: Harald Ringsdörp
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


def find_facts(line):
    """
    Find facts about entities
    input:  line: <http://yago-knowledge.org/resource/EAAT3_Q11856447>	<http://bioschemas.org/isEncodedByBioChemEntity>	<http://yago-knowledge.org/resource/Excitatory_amino_acid_transporter_3>	.
    output: key: EAAT3_Q11856447
            fact: isEncodedByBioChemEntity
            value: Excitatory_amino_acid_transporter_3
    """
    parser = re.findall("/[^/]*>", line)
    # Some lines do not contain three words
    if len(parser) < 3: return None, None, None
    
    key = parser[0][1:-1].replace('_', ' ')
    fact = parser[1][1:-1]
    value = parser[2][1:-1].replace('_', ' ')

    return key, fact, value


def main():
    directory = '../data/'
    # number of largest entities types for creating gazetters
    num_entities = 3
    # files with entities types
    typefiles = ['yago-wd-full-types.nt', 'yago-wd-simple-types.nt']
    # file with labeles
    labelfiles = ['yago-wd-labels.nt']
    factfiles = ['yago-wd-facts.nt']

    # dictionaries to save the data
    all_types = {}
    all_entities = {}
    largest_entities = []
    gazetter = {}

    es = Elasticsearch()
    num = 1 # index number

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

    # find largest types
    for key in all_entities:
        if len(largest_entities) < num_entities:
            largest_entities.append([all_entities[key], len(all_entities[key]), key])
            largest_entities.sort(reverse=True, key = lambda x: x[1])
        elif len(all_entities[key]) > largest_entities[2][1]:
            largest_entities.pop()
            largest_entities.append([all_entities[key], len(all_entities[key]), key])
            largest_entities.sort(reverse=True, key = lambda x: x[1])
    
    # init gazetter
    for i in range(num_entities):
        gazetter[largest_entities[i][2]] = {}
        for item in largest_entities[i][0]:
            gazetter[largest_entities[i][2]][item] = {}

    # import entities to gazetter
    for filename in factfiles:
        with open(f"../data/{filename}") as file:
            line = file.readline()

            while line:
                key, fact, value = find_facts(line)

                if key and key in gazetter[largest_entities[0][2]]:
                    gazetter[largest_entities[0][2]][key][fact] = value
                elif key and key in gazetter[largest_entities[1][2]]:
                    gazetter[largest_entities[1][2]][key][fact] = value
                elif key and key in gazetter[largest_entities[2][2]]:
                    gazetter[largest_entities[2][2]][key][fact] = value

                line = file.readline()

    # indexing by ES, for entity search
    for key in all_entities:
        actions = []

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

    # indexing dictionaries
    for i in range(num_entities):
        actions = []
        num = 1 

        for key in gazetter[largest_entities[i][2]]:
            action = {
                "_index": largest_entities[i][2].lower().replace(' ', ''),
                "_id": num,
                "_source": {
                    "info": gazetter[largest_entities[i][2]][key]
                }
            }

            num += 1 
            actions.append(action)
            
            # index after 0,5M records
            if not num % 500000:
                print(actions)
                helpers.bulk(es, actions)
                actions = []
    
        # index batch
        helpers.bulk(es, actions)


if __name__ == "__main__":
  main()