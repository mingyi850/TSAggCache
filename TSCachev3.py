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
    def __init__(self, groupKeys: Dict, data):
        self.groupKeys = groupKeys
        self.data = data

class Series:
    def __init__(self, groups: Set[str], aggFn: str, aggInterval: int, seriesKey: str, rangeStart: int, rangeEnd: int, data):
        self.seriesKey = seriesKey
        self.groupKeys = groups
        self.aggFn = aggFn
        self.aggInterval = aggInterval
        self.rangeStart = rangeStart
        self.rangeEnd = rangeEnd 
        self.data = data # A List of SeriesGroups

    def __str__(self):
        return f"Table: {self.tableKey}, Group: {self.groupKeys}, AggFn: {self.aggFn}"
    
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





        
    