

from typing import List
from queryDSL import QueryFilter


class InverseIndex:
    def __init__(self):
        self.index = {}

    def add(self, word: str, doc_id: int):
        if word in self.index:
            self.index[word].add(doc_id)
        else:
            self.index[word] = {doc_id}

    def searchWord(self, word: str):
        return self.index.get(word, set())

    #Used for looking up entries in cache which fit the query criteria
    def search(self, queryFilter: QueryFilter):
        if queryFilter.type == "raw":
            return self.searchWord(queryFilter.value)
        elif queryFilter.type == "or":
            #union all values in list
            return set().union(*[self.search(f) for f in queryFilter.filters])
        elif queryFilter.type == "and":
            #intersection of all values in list
            return set.intersection(*[self.search(f) for f in queryFilter.filters])
    
    #Used for finding exact mappings of returned table keys to cache keys
    def findSeries(self, keys: List[str]):
        return set.intersection(*[self.searchWord(key) for key in keys])

    def __str__(self):
        return str(self.index)