import nltk
import re
from elasticsearch import Elasticsearch
import textdistance
from beautifultable import BeautifulTable

def search(entity, es):
  search_param = {"query": {"match": {"pattern": entity}}}
  return es.search(index="entity_idx", body = search_param)['hits']['hits']


def find(words):
  words = re.findall('[a-zA-Z0-9]*/', words)
  return str(''.join([x for x in words])).replace("/", " ")[:-1]


def similarity(type, a, b):
  if type == 'hamming':
    return textdistance.hamming.normalized_similarity(a, b)
  elif type == 'levenshtein':
    return textdistance.levenshtein.normalized_similarity(a, b)
  elif type == 'jaro_winkler':
    return textdistance.jaro_winkler(a, b)
  elif type == 'jaccard':
    tokens_1 = a.split()
    tokens_2 = b.split()
    return textdistance.jaccard(tokens_1 , tokens_2)
  elif type == 'sorensen':
    tokens_1 = a.split()
    tokens_2 = b.split()
    return textdistance.sorensen(tokens_1 , tokens_2)
  elif type == 'ratcliff_obershelp':
    return textdistance.ratcliff_obershelp(a, b)


def evaluate(entity_list, similarity_type):
  table = BeautifulTable()
  table.columns.header = ['similarity type', 'entity type', 'text string', 'entity name', 'percentage']
  
  for entity in entity_list:
    string = entity[0]
    finded_entities = entity[1]
    max = 0
    label = ''
    pattern = ''

    for item in finded_entities:
      item_label = item['_source']['label']
      item_pattern = item['_source']['pattern']
      percentage = similarity(similarity_type, string, item_pattern)

      if max < percentage:
        max = percentage
        label = item_label
        pattern = item_pattern
  
    table.rows.append([similarity_type, label, string, pattern, max])

  print(table)


def main():
  entity_list = []
  similarity_types = ['hamming', 'levenshtein', 'jaro_winkler', 'jaccard', 'sorensen', 'ratcliff_obershelp']
  grammar = r"""NP: {<DT|CD|FW|JJ|NN.*>+}          # Chunk sequences of DT (a, the every nejaky druh), JJ (pridavne meno), NN(podstatne meno)"""
  sentence_data = "The First sentence is about Harald Ringstorff. The Second: about Django. You can learn Python,Django and Data Ananlysis here. "
 
  es = Elasticsearch()

  sentences = nltk.sent_tokenize(sentence_data)
  sentences = [nltk.word_tokenize(sent) for sent in sentences]
  sentences = [nltk.pos_tag(sent) for sent in sentences]

  cp = nltk.RegexpParser(grammar)
  sentences = [cp.parse(s) for s in sentences]

  for sentence in sentences:
    for words in sentence:
      if type(words) is not tuple:
        entity = find(str(words))
        res = search(entity, es)
        if res: entity_list.append([entity, res])

  for st in similarity_types:
    print(st)
    evaluate(entity_list, st)
    print('\n')


if __name__ == "__main__":
  main()
# https://itnext.io/string-similarity-the-basic-know-your-algorithms-guide-3de3d7346227