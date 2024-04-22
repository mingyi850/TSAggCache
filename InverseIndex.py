

from typing import List, Set
from queryDSL import QueryFilter


class InverseIndex:
    def __init__(self):
        self.index = {"filter": {},
                      "aggregation": {},
                      "group": {},
                      "measurements": {}
                    }


    def add(self, type: str, word: str, doc_id: int):
        if type in self.index:
            typeIndex = self.index[type]
            if word in typeIndex:
                typeIndex[word].add(doc_id)
            else:
                typeIndex[word] = {doc_id}
        else:
            self.index[type] = {word: {doc_id}}

    def addList(self, type: str, words: List[str], doc_id: int):
        for word in words:
            self.add(type, word, doc_id)

    def addSeries(self, decomposed_query: dict, series_id: int):
        for key, value in decomposed_query.items():
            if isinstance(value, str) and key in self.index:
                self.add(key, value, series_id)
            elif isinstance(value, list) and key in self.index:
                self.addList(key, value, series_id)
        
    def searchWord(self, type: str, word: str) -> Set[int]:
        if type in self.index:
            return self.index[type].get(word, set())
        else:
            return set()
        
    def searchList(self, type: str, words: List[str]) -> Set[int]:
        return set.intersection(*[self.searchWord(type, word) for word in words])

    #Used for looking up entries in cache which fit the query criteria
    def search(self, queryFilter: QueryFilter):
        if queryFilter.type == "raw":
            return self.searchWord("filter", queryFilter.value)
        elif queryFilter.type == "or":
            #union all values in list
            return set().union(*[self.search(f) for f in queryFilter.filters])
        elif queryFilter.type == "and":
            #intersection of all values in list
            return set.intersection(*[self.search(f) for f in queryFilter.filters])
    
    #Used for finding exact mappings of returned table keys to cache keys
    def findSeries(self, keys: List[str]):
        return set.intersection(*[self.searchWord("filter", key) for key in keys])
    
    def match(self, props: dict):
        sets = []
        for key, value in props.items():
            if key in self.index:
                if isinstance(value, str):
                    sets.append(self.searchWord(key, value))
                elif isinstance(value, list):
                    sets.append(self.searchList(key, value))
        return set.intersection(*sets)


    def __str__(self):
        return str(self.index)