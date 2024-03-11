from queryDSL import InfluxQueryBuilder, QueryAggregation, BaseQueryFilter, Range

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
        self.cache[key].start = result[0].time
        self.cache[key].end = result[-1].time

    def get(self, key, start, end):
        queryStart = start
        queryEnd = end
        appendStart = False
        if key in self.cache:
            entry = self.cache[key]
            if start >= entry.start and end <= entry.end:
                startIndex = (start - entry.start) // entry.aggWindow
                endIndex = (end - entry.start) // entry.aggWindow
                data = entry.data[startIndex:endIndex]
                queryStart = -1
                queryEnd = -1
            elif start >= entry.start and end > entry.end:
                startIndex = (start - entry.start) // entry.aggWindow
                queryStart = entry.data[-2].time
                queryEnd = end
                data = entry.data[startIndex:-1]
            elif start < entry.start and end <= entry.end:
                endIndex = (end - entry.start) // entry.aggWindow
                queryStart = start
                queryEnd = entry.data[0].time
                appendStart = True
                data = entry.data[0:endIndex]
            else:
                queryStart = start
                queryEnd = end
                data = []
            return CacheQueryResult(data, queryStart, queryEnd, appendStart) 
        else:
            return None

    




class CacheKeyGen:
    @staticmethod
    def getKey(self, json):
        return self.getBucketKey(json) + self.getFiltersKey(json) + self.getYield(json) + self.getAggKey(json)

    @staticmethod
    def getBucketKey(self, json) -> str:
        return json["bucket"]
    
    @staticmethod
    def getFiltersKey(self, json) -> str:
        filterList = [BaseQueryFilter.fromJson(f) for f in json["filters"]]
        return ",".join([f.toKey() for f in filterList])
    
    @staticmethod
    def getYield(self, json) -> str:
        return json["yield"]
    
    @staticmethod
    def getAggregation(self, json) -> QueryAggregation:
        return QueryAggregation.fromJson(json["aggregate"])
    
    @staticmethod
    def getAggKey(self, json) -> QueryAggregation:
        return json["aggregate"].timeWindow + json["aggregate"].aggFunc + json["aggregate"].createEmpty
    
    @staticmethod
    def getRange(self, json) -> Range:
        return Range.fromJson(json["range"])
    

    