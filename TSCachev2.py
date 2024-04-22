from typing import List, Set
from queryDSL import InfluxQueryBuilder, QueryAggregation, BaseQueryFilter, QueryFilter, Range
from FluxTableUtils import getTableKeys, getTableListSliced, toTimestamp, getStartTime, getEndTime, getSecondEndTime
from InverseIndex import InverseIndex

'''
Whats the plan for the cache?
1. We will store each series as a separate DataFrame
2. Each query will be queried via a bucket key (this is the name of the table etc: monitoring)
3. Each bucket will have a list of series and an inverse index
4. A query can be broken down into several parts:
    a. Time Range
    b. Measurement (e.g cpu_usage)
    c. filters (e.g key = measurements )
    d. aggregation
    e. group keys
    d. Each series will hold a subset of the data which share measurements, aggregation, time ranges

5. Initial Idea, keep entire query, with query key and search only if range has changed. 
    If range has changed, we will search for the series which match the query and then fetch the data from the series
5.1 Now, we want to handle changes on multiple dimensions 
    Measurements:
        If we search for 2 measurements, and we have cached one measurement, we need to pull the new measurement into the same dataframe.
    Filters: 
        If we search for filters (a AND b AND c) and we cached data for (a AND b), we need to re-query 
        If we search for filters (a AND b) and we have cached data for (a AND b AND c), we only need to pull data where (a AND b AND NOT(c))
    Aggregation:
        If we search for an aggregation of "x" and we have cached data for aggregation "y":
            - First check if aggregation is associative: (mean, max, min, sum, count) or not: (median) (else break)
            - Then check if y < x (else break)
            - Then check x % y == 0 (else break)
            - If all conditions are met, we can combine cached data and return it.
    Group Keys:
        If we search for group keys (a, b, c) and we have cached data for (a, b) and not c, we need to re-query
        If we search for group keys (a, b) and we have cached data for (a, b, c), we can combine them into (a, b) groups.
    Time Range:
        - Self explanatory
6. Query Strategy
    - Since we have 5 conditions, which are constructed as AND statements and we can handle 1 failed condition, we can construct a query strategy as follows:
        Say we have query (TR = 0 - 10) AND (M = cpu_usage) AND (F = (platform = mac_os) OR (platform = windows)) AND (A = mean) AND (G = host)
        We can construct 5 queries, each with 1 condition missing:
        1. (TR = 0 - 10) AND (M = cpu_usage) AND (F = (platform = mac_os) OR (platform = windows)) AND (A = mean)
        2. (TR = 0 - 10) AND (M = cpu_usage) AND (F = (platform = mac_os) OR (platform = windows)) AND (G = host)
        3. (TR = 0 - 10) AND (M = cpu_usage) AND (A = mean) AND (G = host)
        4. (TR = 0 - 10) AND (F = (platform = mac_os) OR (platform = windows)) AND (A = mean) AND (G = host)
        5. (M = cpu_usage) AND (F = (platform = mac_os) OR (platform = windows)) AND (A = mean) AND (G = host)

        for each cache query, we can check if the remaining condition is reconciliable with the remaining data. 
        If it is, we can combine the data and make a second query to the DB.
        If it is not, we can re-query the data and store it in the cache.

        We can then combine the results of these queries to get the final result.

'''
#Denotes a series of points with a fixed combination of keys. Wrapper around a FluxTable
class Series:
    def __init__(self, fluxTable, query: InfluxQueryBuilder, queryStart: int, queryEnd: int, aggWindow: int, data=[]):
        self.keys = getTableKeys(fluxTable)
        self.query = query,
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
        return 


#Denotes a single bucket in the cache. This is the primary entrypoint in the cache. Cross-bucket searches are not allowed
class Bucket:
    def __init__(self, key):
        self.key = key #Key will be the table being searched ('e.g cpu_usage')
        # Holds a list of Series data
        self.count = 0
        self.data = {}
        self.inverseIndex = InverseIndex()
    
    def findMatchingMeasurements(self, measurements):
        partial_matches = [self.inverseIndex.searchWord(m) for m in measurements]
        full_matches = set.intersection(*partial_matches)
        partial_matches = set.union(*partial_matches)
        result = set()
        for m in partial_matches:
            if m not in full_matches:
                result.add((m, ["measurement"]))
        for m in full_matches:
            result.add((m, None))
        return result
        
    
    def findMatchingSeries(self, filter) -> Set[int]:
        return self.inverseIndex.match(filter)
        
    def fetchData(self, dataKeys: Set[int]):
        return [self.data[key] for key in dataKeys]
    
class TsCacheV2:
    def __init__(self):
        self.cache = dict()

    def reset(self):
        self.cache = dict()

    def set(self, key, result, requestJson):
        return
    def insert(self, requestJson, result):
        return
    def get(self, json):
        decomposed = CacheKeyGenv2.decomposeQuery(json)
        queryDSL = InfluxQueryBuilder.fromJson(json)
        bucket = self.cache.get(CacheKeyGenv2.getBucketKey(json))
        if bucket is None:
            return None
        fullMatch = bucket.findMatchingSeries(decomposed)
        matchingSeries = [bucket.data[key] for key in fullMatch]
        for series in matchingSeries:
            result, query = self.is_series_usable(series, decomposed, None)
            if result is True:
                return series.getRange(query["range"].start, query["range"].end), query
        else:
            for key in ["filters", "groupKeys"]:
                partial_decomposed = {key: value for key, value in dict.items() if key != key}
                partialMatch = bucket.match(partial_decomposed)
                for series in partialMatch:
                    result, query = self.is_series_usable(series, decomposed, key)
                    if result is True:
                        return series.getRange(query["range"].start, query["range"].end), query
            return None

    #Checks if a series is usable and if the remaining values can be reconciled with a single query
    def is_series_usable(self, series: Series, decomposed: dict, removed_key: dict, query: InfluxQueryBuilder):
        series_decomposed = decomposed
        keys = ["filters", "groupKeys", "aggregation", "measurements"].remove(removed_key)
        for key in keys:
            if series_decomposed[key] != decomposed[key]:
                return False, None
        valid_range = self.validateRange(series, decomposed, removed_key)
        if valid_range is False:
            return False, None

        return False
    
    def validateAggregations(self, working_set, bucket, query_aggregation):
        new_working_set = set()
        for index, partialled in working_set:
            series = bucket.data[index]
            if self.validateAggregation(series, partialled, query_aggregation) is True:
                new_working_set.add((index, partialled))
        return new_working_set
    
    def validateAggregation(self, series: Series, partialled: str, query_aggregation):
        if series.aggFunc == query_aggregation.aggFunc:
            if query_aggregation.aggFunc in ["mean", "max", "min", "sum", "count"]:
                if query_aggregation.getTimeWindowSeconds() % series.aggWindow == 0 and query_aggregation.getTimeWindowSeconds() > series.aggWindow:
                    return True
                else:
                    return False
            if query_aggregation.aggFunc == "median":
                return False
        else:
            return False
    
    def validateRanges(self, working_set, bucket, queryDSL):
        new_working_set = set()
        for index, partialled in working_set:
            series = bucket.data[index]
            validationResult = self.validateRange(series, partialled, queryDSL)
            if validationResult == "Matched":
                new_working_set.add((index, partialled))
            elif validationResult == "Partial" and partialled is None:
                new_working_set.add((index, "range"))
        return new_working_set
    
    def validateRange(self, series: Series, queryDSL: InfluxQueryBuilder):
        if series.queryStart >= queryDSL.range.start and series.queryEnd <= queryDSL.range.end:
            return "Matched"
        if series.queryStart >= queryDSL.range.start and series.queryEnd > queryDSL.range.end:
            return "Partial"
        if series.queryStart < queryDSL.range.start and series.queryEnd <= queryDSL.range.end:
            return "Partial"
        else:
            return "NoMatch"
    
    # Returns a raw list of AND filters
    def breakdownAndFilter(self, filter: QueryFilter):
        if filter.type == "raw":
            return [filter]
        if filter.type == "and":
            return [self.breakdownAndFilter(f) for f in filter.filters]
    
    def breakdownOrFilter(self, filter: QueryFilter):
        if filter.type == "raw":
            return [filter]
        elif filter.type == "and":
            return [self.breakdownAndFilter(f) for f in filter.filters]
        if filter.type == "or":
            return [self.breakdownOrFilter(f) for f in filter.filters]

    #Incomplete implementation
    def getDNF(self, filters: List[QueryFilter]):
        result = [{}]
        filterSet = set()
        for filter in filters:
            if filter.type == "and":
                breakdown = self.breakdownAndFilter(filter)
                for existing in result:
                    for b in breakdown:
                        existing.add(b.toKey)
            if filter.type == "raw":
                for existing in result:
                    existing.add(filter.toKey)
            else: #OR filter
                breakdown = self.breakdownOrFilter(filter)
                new_result = []
                for b in breakdown:
                    for existing in result:
                        new_result.append(existing + b)
                result = new_result
        return result

                
            
    #TODO: If we can convert to DNF, we can use subset logic to check for partial matches
    def validateFilter(self, series: Series, searchQuery: InfluxQueryBuilder):
        seriesFilters = set(series.query.filters)
        searchFilters = set(searchQuery.filters)
        if seriesFilters == searchFilters:
            return "Matched"
        else:
            return "NoMatch"
    
    def validateFilters(self, working_set, bucket, queryDSL):
        new_working_set = set()
        for index, partialled in working_set:
            series = bucket.data[index]
            validationResult = self.validateFilter(series, queryDSL)
            if validationResult == "Matched":
                new_working_set.add((index, partialled))
            elif validationResult == "Partial" and partialled is None:
                new_working_set.add((index, "filters"))
        return new_working_set
    
    def validateGroup(self, series: Series, queryDSL: InfluxQueryBuilder):
        seriesGroupKeys = set(series.query.groupKeys)
        searchGroupKeys = set(queryDSL.groupKeys)
        if seriesGroupKeys == searchGroupKeys:
            return "Matched"
        elif searchGroupKeys.issubset(seriesGroupKeys):
            return "Partial"
        else:
            return "NoMatch"
        
    def validateGroups(self, working_set, bucket, queryDSL):
        new_working_set = set()
        for index, partialled in working_set:
            series = bucket.data[index]
            validationResult = self.validateGroup(series, queryDSL)
            if validationResult == "Matched":
                new_working_set.add((index, partialled))
            elif validationResult == "Partial" and partialled is None:
                new_working_set.add((index, "groupKeys"))
        return new_working_set
    
    def searchCache(self, json):
        bucket = self.cache.get(CacheKeyGenv2.getTableKey(json), None)
        queryDsl = InfluxQueryBuilder.fromJson(json)
        query_range = CacheKeyGenv2.getRange(json)
        query_aggregation = CacheKeyGenv2.getAggregation(json)
        if bucket is None:
            return None
        working_set = bucket.findMatchingMeasurements(CacheKeyGenv2.getMeasurementsKey(json))
        working_set = self.validateAggregations(working_set, bucket, query_aggregation)
        working_set = self.validateRanges(working_set, bucket, query_range)
        working_set = self.validateFilters(working_set, bucket, queryDsl)
        working_set = self.validateGroups(working_set, bucket, queryDsl)
        return working_set
    
    def reconcileSeries(self, series: Series, partialled: list[str], searchQuery: InfluxQueryBuilder):
        return





    
    

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
    
    @staticmethod
    def getGroupKeys(json) -> List[int]:
        return json["groupKeys"]
    
    @staticmethod
    def getAggregationTypeKey(json) -> str:
        return f"agg={CacheKeyGenv2.getAggregation(json).aggFunc}"
    
    def getAggregationWindowKey(json) -> str:
        return f"window={CacheKeyGenv2.getAggregationWindow(json)}"
    
    def getAggregationKey(json) -> str:
        return CacheKeyGenv2.getAggregationTypeKey(json) + CacheKeyGenv2.getAggregationWindowKey(json)
    
    def getInverseIndexFilters(json):
        filters = json["filters"]
        filters = sorted(filters, key=lambda x: x["key"])
        return [BaseQueryFilter.fromJson(filter).toKey for filter in filters]
    
    def getMeasurementsKey(json) -> str:
        return json["measurements"]
    
    def getTableKey(json) -> str:
        return json["table"]

    def decomposeQuery(self, json):
        range = CacheKeyGenv2.getRange(json)
        table = CacheKeyGenv2.getTableKey(json)
        measurements = CacheKeyGenv2.getMeasurementsKey(json)
        filters = CacheKeyGenv2.getInverseIndexFilters(json)
        groupKeys = CacheKeyGenv2.getGroupKeys(json)
        aggregation = CacheKeyGenv2.getAggregationKey(json)

        return {
            "table": table,
            "range": range,
            "measurements": measurements,
            "filters": filters,
            "groupKeys": groupKeys,
            "aggregation": aggregation
        }


    


    