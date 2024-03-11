from queryDSL import InfluxQueryBuilder, QueryAggregation, BaseQueryFilter, Range
from FluxTableUtils import getTableListSliced, toTimestamp, getStartTime, getEndTime, getSecondEndTime
'''
[5, 10, 15, 20, 25] 
if we have time stamp of 7 -> we want from 5 onwards 
(7 - 5 // 5) -> 0
if we have time stamp of 11 -> we want from 10 onwards 
(11 - 5) // 5 -> 1
'''
class CacheEntry:
    def __init__(self, key, start, end, aggWindow, data):
        self.key = key
        self.start = start
        self.end = end
        self.aggWindow = aggWindow
        self.data = []

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

    def set(self, key, result):
        self.cache[key].data = result
        self.cache[key].start = toTimestamp(getStartTime(result))
        self.cache[key].end = toTimestamp(getEndTime(result))

    def insert(self, requestJson, result):
        key = CacheKeyGen.getKey(requestJson)
        if key in self.cache:
            self.set(key, result)
        else:
            entry = CacheEntry(key, toTimestamp(getStartTime(result)), toTimestamp(getEndTime(result)), CacheKeyGen.getAggregation(requestJson).getTimeWindowSeconds(), result)
            self.cache[key] = entry

    def get(self, json):
        key = CacheKeyGen.getKey(json)
        range = CacheKeyGen.getRange(json)
        start = range.start
        end = range.end

        queryStart = start
        queryEnd = end
        appendStart = False
        if key in self.cache:
            entry = self.cache[key]
            print("Cache hit", entry.start, entry.end, json.get("range"))
            if len(entry.data) == 0:
                return None
            if start >= entry.start and end <= entry.end:
                startIndex = (start - entry.start) // entry.aggWindow
                endIndex = (end - entry.start) // entry.aggWindow
                data = getTableListSliced(entry.data, startIndex, endIndex)
                queryStart = -1
                queryEnd = -1
            elif start >= entry.start and end > entry.end:
                startIndex = (start - entry.start) // entry.aggWindow
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

    

class CacheKeyGen:
    @staticmethod
    def getKey(json):
        return CacheKeyGen.getBucketKey(json) + CacheKeyGen.getFiltersKey(json) + CacheKeyGen.getYield(json) + CacheKeyGen.getAggKey(json)

    @staticmethod
    def getBucketKey(json) -> str:
        return json["bucket"]
    
    @staticmethod
    def getFiltersKey(json) -> str:
        filterList = [BaseQueryFilter.fromJson(f) for f in json["filters"]]
        return ",".join([f.toKey() for f in filterList])
    
    @staticmethod
    def getYield(json) -> str:
        return json["yield"]
    
    @staticmethod
    def getAggregation(json) -> QueryAggregation:
        return QueryAggregation.fromJson(json["aggregate"])
    
    @staticmethod
    def getAggKey(json) -> QueryAggregation:
        return json["aggregate"]["timeWindow"] + json["aggregate"]["aggFunc"] + str(json["aggregate"]["createEmpty"])
    
    @staticmethod
    def getRange(json) -> Range:
        return Range.fromJson(json["range"])
    

    