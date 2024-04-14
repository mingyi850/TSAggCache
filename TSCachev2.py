from typing import Set
from queryDSL import InfluxQueryBuilder, QueryAggregation, BaseQueryFilter, Range
from FluxTableUtils import getTableKeys, getTableListSliced, toTimestamp, getStartTime, getEndTime, getSecondEndTime
from InverseIndex import InverseIndex

class CacheEntry:
    def __init__(self, key, start, end, aggWindow, data):
        self.key = key
        self.start = start
        self.end = end
        self.aggWindow = aggWindow
        self.data = data

    def __repr__(self):
        return f"CacheEntry({self.key}, {self.start}, {self.end}, {self.aggWindow}, {self.data})"

class CacheQueryResult:
    def __init__(self, data, queryStart, queryEnd, appendStart=False):
        self.data = data
        self.queryStart = queryStart
        self.queryEnd = queryEnd
        self.appendStart = appendStart

class DataPoint:
    def __init__(self, time, value):
        self.time = time
        self.value = value
    
class MiniTSCache:
    def __init__(self):
        self.cache = dict()

    def reset(self):
        self.cache = dict()

    def set(self, key, result, requestJson):
        self.cache[key].data = result
        endTime = toTimestamp(getEndTime(result))
        secondEndTime = toTimestamp(getSecondEndTime(result))
        # required adjustment - last timestamp from query will be the end time of the query. If last timestamp only encapsulates 40 seconds of data, first timestamp returned will increased by 20s
        adjustment = CacheKeyGenv2.getAggregationWindow(requestJson) - (endTime - secondEndTime) + 1
        self.cache[key].start = toTimestamp(getStartTime(result)) - adjustment
        self.cache[key].end = endTime


    def insert(self, requestJson, result):
        key = CacheKeyGenv2.getKey(requestJson)
        if key in self.cache:
            print("setting cache", result)
            self.set(key, result, requestJson)
        else:
            print("creating cache entry")
            print("Result to set: ", result)
            entry = CacheEntry(key, toTimestamp(getStartTime(result)), toTimestamp(getEndTime(result)), CacheKeyGenv2.getAggregation(requestJson).getTimeWindowSeconds(), result)
            print(entry)
            self.cache[key] = entry

    def get(self, json):
        key = CacheKeyGenv2.getKey(json)
        range = CacheKeyGenv2.getRange(json)
        start = range.start
        end = range.end

        queryStart = start
        queryEnd = end
        appendStart = False
        if key in self.cache:
            entry = self.cache[key]
            print("Cache hit", entry.start, entry.end, start, end)
            if len(entry.data) == 0:
                print("No data found in entry despite hit")
                return None
            if start >= entry.start and end <= entry.end:
                startIndex = (start - entry.start) // entry.aggWindow
                endIndex = (end - entry.start) // entry.aggWindow
                data = getTableListSliced(entry.data, startIndex, endIndex)
                queryStart = -1
                queryEnd = -1
            elif start >= entry.start and end > entry.end:
                startIndex = (start - entry.start) // entry.aggWindow
                print("Looking at startIndex of ", startIndex)
                queryStart = toTimestamp(getSecondEndTime(entry.data))
                queryEnd = end
                data = getTableListSliced(entry.data, startIndex, -1)
            elif start < entry.start and end <= entry.end:
                endIndex = (end - entry.start) // entry.aggWindow
                queryStart = start
                queryEnd = toTimestamp(getStartTime(entry.data))
                appendStart = True
                data = getTableListSliced(entry.data, 0, endIndex)
            else:
                return None
            return CacheQueryResult(data, queryStart, queryEnd, appendStart) 
        else:
            return None

class RangeResult:
    def __init__(self, fluxTable, aggIntervalInt, complete=False):
        if fluxTable is None:
            self.complete = False
            self.data = None
            self.start = -1
            self.end = -1
        else:
            self.start = toTimestamp(getStartTime(fluxTable)) - aggIntervalInt
            self.end = toTimestamp(getEndTime(fluxTable))
            self.data = fluxTable
            self.complete = complete
        
    def __repr__(self):
        return f"RangeResult({self.start}, {self.end})"
    
    def __eq__(self, other):
        return self.start == other.start and self.end == other.end
#Denotes a series of points with a fixed combination of keys. Wrapper around a FluxTable
class Series:
    def __init__(self, fluxTable, queryStart: int, queryEnd: int, aggWindow: int):
        self.keys = getTableKeys(fluxTable)
        self.data = []
        self.queryStart = queryStart
        self.queryEnd = queryEnd
        self.dataStart = toTimestamp(getStartTime(fluxTable))
        self.dataEnd = toTimestamp(getEndTime(fluxTable))
        self.aggWindow = aggWindow
    
    def set(self, data, queryStart, queryEnd):
        self.queryStart = min(queryStart, self.queryStart)
        self.queryEnd = max(queryEnd, self.queryEnd)
        self.dataStart = toTimestamp(getStartTime(data))
        self.dataEnd = toTimestamp(getEndTime(data)) 
        self.data = data

    def getRange(self, start, end):
        if start >= self.queryStart and end <= self.queryEnd:
            return RangeResult(getTableListSliced(self.data, (start - self.queryStart) // self.aggWindow, (end - self.queryStart) // self.aggWindow), self.aggWindow, True)
        elif start < self.queryStart and end <= self.queryEnd:
            endIndex = (end - self.dataStart) // self.aggWindow
            return RangeResult(getTableListSliced(self.data, 1, endIndex), self.aggWindow)
        elif start >= self.queryStart and end > self.queryEnd:
            startIndex = (start - self.dataStart) // self.aggWindow
            return RangeResult(getTableListSliced(self.data, startIndex, -1), self.aggWindow)
        else:
            return RangeResult(None, self.aggWindow)
        
    
    def getKeys(self):
        return self.keys
    
    

#Denotes a single bucket in the cache. This is the primary entrypoint in the cache. Cross-bucket searches are not allowed
class Bucket:
    def __init__(self, key):
        self.key = key
        # Holds a list of Series data
        self.data = []
        self.inverseIndex = InverseIndex()
    
    def findMatchingSeries(self, filter):
        return self.inverseIndex.search(filter)
        
    def fetchData(self, dataKeys: Set[int]):
        return [self.data[key] for key in dataKeys]
    

        

    

class CacheKeyGenv2:
    @staticmethod
    def getKey(json) -> str:
        return CacheKeyGenv2.getBucketKey(json)

    @staticmethod
    def getBucketKey(json: str) -> str:
        return json["bucket"]
    
    @staticmethod
    def getAggregation(json) -> QueryAggregation:
        return QueryAggregation.fromJson(json["aggregate"])
    
    @staticmethod
    def getAggregationWindow(json) -> int:
        return CacheKeyGenv2.getAggregation(json).getTimeWindowSeconds()
    
    @staticmethod
    def getRange(json) -> Range:
        return Range.fromJson(json["range"])
    

    

    