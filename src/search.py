from spacy.lang.en import English
from spacy.pipeline import EntityRuler
from elasticsearch import Elasticsearch

nlp = English()
ruler = EntityRuler(nlp)

es = Elasticsearch()

body = {
    "query" : {
        "match_all" : {}
    },
    "size": 10000
}


number = es.cat.count('entity_idx', params={"format": "json"})[0]['count']
print(number)

patterns = []

for i in range(1, int(number)+1):
    # print(es.get(index="entity_idx", id=i)['_source'])
    patterns.append(es.get(index="entity_idx", id=i)['_source'])

# patterns = [{"label": "ORG", "pattern": "Apple"},
#             {"label": "GPE", "pattern": [{"LOWER": "san"}, {"LOWER": "francisco"}]}]
ruler.add_patterns(patterns)
nlp.add_pipe(ruler)

doc = nlp("Doug Wenn is opening its first case de fuite in Fry Peaks.")
print([(ent.text, ent.label_) for ent in doc.ents])