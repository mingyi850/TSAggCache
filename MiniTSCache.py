from queryDSL import InfluxQueryBuilder, QueryAggregation, BaseQueryFilter, Range
from FluxTableUtils import getTableListSliced, toTimestamp, getStartTime, getEndTime, getSecondEndTime
'''
[5, 10, 15, 20, 25] 
if we have time stamp of 7 -> we want from 5 onwards 
(7 - 5 // 5) -> 0
if we have time stamp of 11 -> we want from 10 onwards 
(11 - 5) // 5 -> 1

TODO: Think of way to handle empty data. Assumption in implementation is that data is always present and consistent.
However, sometimes if data is not produced, timestamp of first value returned from query will be higher than the start time of the query.
This means that consecutive queries will cause a cache miss since firstTime > queryStart.
Maybe we can fill with null values
'''
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
        adjustment = CacheKeyGen.getAggregationWindow(requestJson) - (endTime - secondEndTime) + 1
        self.cache[key].start = toTimestamp(getStartTime(result)) - adjustment
        self.cache[key].end = endTime


    def insert(self, requestJson, result):
        key = CacheKeyGen.getKey(requestJson)
        if key in self.cache:
            print("setting cache", result)
            self.set(key, result, requestJson)
        else:
            print("creating cache entry")
            print("Result to set: ", result)
            entry = CacheEntry(key, toTimestamp(getStartTime(result)), toTimestamp(getEndTime(result)), CacheKeyGen.getAggregation(requestJson).getTimeWindowSeconds(), result)
            print(entry)
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
    def getAggregationWindow(json) -> int:
        return CacheKeyGen.getAggregation(json).getTimeWindowSeconds()
    
    @staticmethod
    def getAggKey(json) -> QueryAggregation:
        return json["aggregate"]["timeWindow"] + json["aggregate"]["aggFunc"] + str(json["aggregate"]["createEmpty"])
    
    @staticmethod
    def getRange(json) -> Range:
        return Range.fromJson(json["range"])
    

    