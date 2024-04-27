from datetime import datetime
from queryDSL import InfluxQueryBuilder
from queryDSL import AndQueryFilter
import pandas as pd
from typing import Set, List, Dict

''' TO Fetch series
- use table key
- check grouping fits
- check aggFn fits
'''

class SeriesGroup:
    def __init__(self, groupKeys: Dict, data: pd.DataFrame):
        self.groupKeys = groupKeys
        self.data = data

class Series:
    def __init__(self, table: str, groups: Set[str], aggFn: str, aggInterval: int, rangeStart: int, rangeEnd: int, data: Dict[tuple, SeriesGroup]):
        self.table = table
        self.groupKeys = groups
        self.aggFn = aggFn
        self.aggInterval = aggInterval
        self.rangeStart = rangeStart
        self.rangeEnd = rangeEnd 
        self.data = data # A Dict of seriesGroup objects, with keys as tuples of group keys

    def __str__(self):
        return f"Table: {self.table}, Group: {self.groupKeys}, AggFn: {self.aggFn}, Data: {self.data} "
    
    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.tableKey == other.tableKey and self.groupKeys == other.groupKeys and self.aggFn == other.aggFn

    def __hash__(self):
        return hash((self.tableKey, self.groupKeys, self.aggFn))

    def setData(self, data):
        self.data = data

    def getData(self):
        return self.data

    def getTableKey(self):
        return self.tableKey

    def getGroupKeys(self):
        return self.groupKeys

    def getAggFn(self):
        return self.aggFn
    
    def getDataSlices(self, rangeStart: int, rangeEnd: int) -> Dict[str, SeriesGroup]:
        print(f"Range start: {rangeStart}, Range end: {rangeEnd}, Series start: {self.rangeStart}, Series end: {self.rangeEnd}, aggInterval: {self.aggInterval}")
        startIndex = int((rangeStart - self.rangeStart) / self.aggInterval)
        endIndex = int((rangeEnd - self.rangeStart) / self.aggInterval)
        print("KEYS: ", list(self.data.keys()))
        print("Data: ", self.data)
        return {k: self.data[k][startIndex:endIndex] for k in list(self.data.keys())}
        
    
def getTableKey(queryDSL: InfluxQueryBuilder) -> str:
    return queryDSL.table

def getAggFn(queryDSL: InfluxQueryBuilder) -> str:
    return queryDSL.aggregate.aggFunc

def getFiltersKey(queryDSL: InfluxQueryBuilder) -> str:
    return AndQueryFilter(queryDSL.filters).asSetKey()

def getGroupKeys(queryDSL: InfluxQueryBuilder):
    return set(queryDSL.groupKeys)

def getSeriesKey(queryDSL: InfluxQueryBuilder):
    return f"{getTableKey(queryDSL)}_{getAggFn(queryDSL)}_{getFiltersKey(queryDSL)}"

class TSCachev3:
    def __init__(self):
        self.cache = {}
        
    def get(self, queryDSL: InfluxQueryBuilder) -> Series:
        seriesKey = getSeriesKey(queryDSL)
        if seriesKey in self.cache:
            return self.cache[seriesKey]
        else:
            return None
    
    def set(self, queryDSL: InfluxQueryBuilder, series: Series):
        seriesKey = getSeriesKey(queryDSL)
        self.cache[seriesKey] = series





        
    