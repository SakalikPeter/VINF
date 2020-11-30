from beautifultable import BeautifulTable
from elasticsearch import Elasticsearch
import textdistance
import nltk
import re

def search(label, pattern, es):
  """
  Search in ES
  input:  label:Human
          pattern: Tom Cruise
          es: (ES object)
  output: (query response)
  """
  search_param = {"query": {"bool": {"must": [{"match": {"label": {'query': label, 'fuzziness': 2}}}, {"match": {"pattern": {'query': pattern, 'fuzziness': 2}}}]}}}
  return es.search(index="entity_index", body = search_param)['hits']['hits']


def find(words):
  """
  Parse words from pos_tags
  input:  words: (NP The/DT First/JJ sentence/NN)
  output: The First sentence
  """
  words = re.findall('[a-zA-Z0-9]*/', words)
  return str(''.join([x for x in words])).replace("/", " ")[:-1]


def similarity(type, a, b):
  """
  String similarity metrics
  input:  type: hamming (similarity type)
          a: John (string 1)
          b: John Snow (string 2)
  output: 0.73 (probability)
  """
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
  """
  Evaluating finded outputs, sort by similarity and save to file
  input:  entity_list: (ouput from ES)
          similarity_type: jaccard
  """
  # output table directory
  directory = '../output/'
  f = open(f"{directory}{similarity_type}.txt", "w")
  
  for entity in entity_list:
    table = BeautifulTable()
    table.columns.header = ['entity type', 'text string', 'entity name', 'percentage']
    
    output = []
    string = entity[0]
    finded_entities = entity[1]

    output = [[item['_source']['label'], string, item['_source']['pattern'], similarity(similarity_type, string, item['_source']['pattern'])] for item in finded_entities]
    output.sort(reverse=True, key=lambda x: x[3])

    for item in output: table.rows.append([item[0], item[1], item[2], item[3]])
    
    f.write(str(table))
    f.write('\n\n')


def main():
  # finded entities
  entity_list = []
  # metrics
  similarity_types = ['hamming', 'levenshtein', 'jaro_winkler', 'jaccard', 'sorensen', 'ratcliff_obershelp']
  # grammar rule
  grammar = r"""NP: {<DT|CD|FW|JJ|NN.*>+}"""
  # type
  entity_type = input("Enter entity: ")
  # text 
  sentence_data = open("../input.txt", "r").read()
 
  es = Elasticsearch()

  # tokenize sentences
  sentences = nltk.sent_tokenize(sentence_data)
  # tokenize words
  sentences = [nltk.word_tokenize(sent) for sent in sentences]
  # word tagger
  sentences = [nltk.pos_tag(sent) for sent in sentences]

  cp = nltk.RegexpParser(grammar)
  # extract words from sentences
  sentences = [cp.parse(s) for s in sentences]

  # extract entities from extracted words
  for sentence in sentences:
    for words in sentence:
      if type(words) is not tuple:
        entity = find(str(words))
        res = search(entity_type, entity, es)
        if res: entity_list.append([entity, res])

  # evaluate output
  for st in similarity_types:
    evaluate(entity_list, st)


if __name__ == "__main__":
  main()
