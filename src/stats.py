from beautifultable import BeautifulTable
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from collections import Counter
import numpy as np
import operator


def softmax(x):
    """
    Softmax function on numpy array
    input:  x: (numpy array)
    output: (numpy array after softmax)
    """
    mean = np.mean(x)
    std = np.std(x)
    norm = (x - mean) / std
    e_x = np.exp(norm)
    return e_x / e_x.sum(axis=0)


def stats(entities, name, f):
    """
    Stats over entity
    input:  entities: (filtered response)
            name: (ES index field name)
            f: (file)
    """
    table = BeautifulTable()
    table.columns.header = [name, 'percentage']

    entities = sorted(Counter(entities).items(), key=operator.itemgetter(1), reverse=True)[:10]
    x = [x[1] for x in entities]
    x = softmax(np.array(x))
    for item1, item2 in zip(entities, x): table.rows.append([item1[0], item2])
    
    f.write(str(table))
    f.write('\n\n')


def get_gazetter(es, index):
    """
    Get gazetter by index name
    input:  es: (ES object)
            index: (index name)
    output: (query response)
    """
    return scan(
        es,
        index=index,
        query={"query": { "match_all" : {}}}
    )


def filter_stats(es_response, att1, att2, att3):
    """
    Filter response from ES
    input:  es_response: (query response)
            att1: (gazetter field name)
            att2: (gazetter field name)
            att3: (gazetter field name)
    output: l1: (list filtered by field name)
            l2: (list filtered by field name)
            l3: (list filtered by field name)
    """
    l1 = []
    l2 = []
    l3 = []

    for item in es_response:
        if att1 in item['_source']['info']: l1.append(item['_source']['info'][att1])
        if att2 in item['_source']['info']: l2.append(item['_source']['info'][att2])
        if att3 in item['_source']['info']: l3.append(item['_source']['info'][att3])
    
    return l1, l2, l3


def main():
    es = Elasticsearch()

    # place attributes
    place_location = []
    place_contains_place = []
    place_contained_in_place = []
    # person attributes 
    occupation = []
    birthPlace = []
    nationality = []
    # creative work attributes
    language = []
    genre = []
    country = []


    place_location, place_contains_place, place_contained_in_place = filter_stats(get_gazetter(es, 'place'), 'location', 'containsPlace', 'containedInPlace')

    f = open(f"../stats/place.txt", "w")
    stats(place_location, 'location', f)
    stats(place_contains_place, 'place', f)
    stats(place_contained_in_place, 'contained in place', f)
    f.close()


    occupation, birthPlace, nationality = filter_stats(get_gazetter(es, 'person'), 'hasOccupation', 'birthPlace', 'nationality')

    f = open(f"../stats/person.txt", "w")
    stats(occupation, 'occupation', f)
    stats(birthPlace, 'birthPlace', f)
    stats(nationality, 'nationality', f)
    f.close()


    language, genre, country = filter_stats(get_gazetter(es, 'creativework'), 'inLanguage', 'genre', 'countryOfOrigin')

    f = open(f"../stats/creative_work.txt", "w")
    stats(language, 'language', f)
    stats(genre, 'genre', f)
    stats(country, 'country', f)
    f.close()

if __name__ == "__main__":
  main()