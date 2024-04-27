import TSCachev3
from queryDSL import InfluxQueryBuilder, Range
import copy
from influxdb_client_3 import InfluxDBClient3
import pandas as pd
from TSCachev3 import SeriesGroup, Series
from typing import List, Set, Dict, Tuple

INFLUXDB_TOKEN="VJK1PL0-qDkTIpSgrtZ0vq4AG02OjpmOSoOa-yC0oB1x3PvZCk78In9zOAGZ0FXBNVkwoJ_yQD6YSZLx23WElA==" #TODO: Remove or swap
token = INFLUXDB_TOKEN
org = "Realtime Big Data"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

class CacheService:
    def __init__(self):
        self.cache = TSCachev3.TSCachev3()
        self.client = InfluxDBClient3(host=host, token=token, org=org)

    def query(self, json):
        queryDSL = InfluxQueryBuilder.fromJson(json) #Convert to query builder object
        cachedResults = self.cache.get(queryDSL) #Check if a matching entry exists in cache based on table_aggFn_filter
        cachedResults = self.checkGrouping(queryDSL, cachedResults)
        cachedResults = self.checkAggWindow(queryDSL, cachedResults)
        additionalRange = self.checkAdditionalRange(queryDSL, cachedResults)
        if additionalRange is not None:
            print("Additional Range is", additionalRange)
            (newRange, rangeType) = additionalRange
            newQueryDSL = InfluxQueryBuilder.fromJson(json)
            newQueryDSL = self.modifyQuery(newQueryDSL, cachedResults, newRange)
            query = newQueryDSL.buildInfluxQlStr()
            print("Using query", query)
            results = self.client.query(query=query, database=queryDSL.bucket, language="influxql", mode='pandas')
            newSeries = self.combineResults(newQueryDSL, cachedResults, results, rangeType)
            self.cache.set(newQueryDSL, newSeries)
        
        
        # Now we want to give user back what they want
        # - First we slice by time range they need
        newCachedResults = self.cache.get(queryDSL)
        print("New cached results is ", newCachedResults)
        slicedSeries = newCachedResults.getSlicedSeries(queryDSL.range.start, queryDSL.range.end)
        regroupedSeries = slicedSeries.regroup(queryDSL.groupKeys, queryDSL.measurements)
        reAggregatedSeries = regroupedSeries.reAggregate(queryDSL.aggregate.getTimeWindowSeconds(), queryDSL.measurements)
        # - Second we slice by columns they are looking for
        columnedSeries = reAggregatedSeries.filterMeasurements(queryDSL.measurements)
        # - Third we regroup columns based on new groupings
        # - Lastly we re-aggregate results
        #Return the modified value
        combinedResults = columnedSeries.getCombinedDataFrame()
        return combinedResults
    
    def groupDataDict(self, data: pd.DataFrame, groupKeys: list) -> dict:
        result = dict()
        print("Group keys are", groupKeys)
        print("New data is", data)
        sortedGroupKeys = sorted(groupKeys)
        grouped = data.groupby(sortedGroupKeys)
        for key, group in grouped:
            seriesKeyDict = dict()
            for i in range(len(groupKeys)):
                seriesKeyDict[groupKeys[i]] = key[i]
            seriesGroup = SeriesGroup(seriesKeyDict, group)
            result[key] = seriesGroup
        return result
    # Manipulate existing series:
    # - if previous series was none, create new series
    # - if additional range was original range (no overlap or did not exist), append series
    # - if additional range was at the start, append series and update range start
    # - if additional range was at the end, append series and update range end 
    def combineResults(self, newQueryDSL: InfluxQueryBuilder, cachedSeries: TSCachev3.Series, newResults: pd.DataFrame, rangeType: str) -> TSCachev3.Series:
        newSeriesData = self.groupDataDict(newResults, newQueryDSL.groupKeys)
        if rangeType == "full":
            newSeries = TSCachev3.Series(newQueryDSL.table, set(newQueryDSL.groupKeys), newQueryDSL.aggregate.aggFunc, newQueryDSL.aggregate.getTimeWindowSeconds(), newQueryDSL.range.start, newQueryDSL.range.end, newSeriesData)
            return newSeries
        elif rangeType == "start":
            if not newSeriesData:
                return cachedSeries
            existing = cachedSeries.getData()
            for key in existing:
                value = existing[key]
                newSeriesData[key].data = pd.concat([newSeriesData[key].data, value.data])
            newSeries = TSCachev3.Series(newQueryDSL.table, set(newQueryDSL.groupKeys), newQueryDSL.aggregate.aggFunc, newQueryDSL.aggregate.getTimeWindowSeconds(), newQueryDSL.range.start, cachedSeries.rangeEnd, newSeriesData)
            return newSeries
        elif rangeType == "end":
            if not newSeriesData:
                return cachedSeries 
            existing = cachedSeries.getData()
            for key in existing:
                value = existing[key]
                print("Key is", key, "Value is", value, "Data is", value, "New data is", newSeriesData)
                newSeriesData[key].data = pd.concat([value.data, newSeriesData[key].data])
            newSeries = TSCachev3.Series(newQueryDSL.table, set(newQueryDSL.groupKeys), newQueryDSL.aggregate.aggFunc, newQueryDSL.aggregate.getTimeWindowSeconds(), cachedSeries.rangeStart, newQueryDSL.range.end, newSeriesData)
            return newSeries
        

    
    def modifyQuery(self, queryDSL: InfluxQueryBuilder, cachedResults: TSCachev3.Series, newRange: Range) -> InfluxQueryBuilder:
        queryDSL.range = newRange
        queryDSL.measurements = ['*']
        if cachedResults is not None:
            queryDSL.groupKeys = sorted(list(cachedResults.groupKeys))
            queryDSL.aggregate.timeWindow = f"{cachedResults.aggInterval}s"
            queryDSL.aggregate.aggFunc = cachedResults.aggFn
        return queryDSL
    
    '''
                    cache 1500-1600 1600-1700 1700-1800 (1hr windows)
    user requests:  req   1605 - 1805
                          technically what we should return (1605 - 1705) (1705 - 1805)
                          but we want to use cache so we return (1600 - 1700) (1700 - 1800)

    '''
    def checkAdditionalRange(self, queryDSL: InfluxQueryBuilder, cachedResults: TSCachev3.Series) -> Tuple[Range, str]:
        if cachedResults is not None:
            queryStart = queryDSL.range.start
            queryEnd = queryDSL.range.end
            cachedStart = cachedResults.rangeStart
            cachedEnd = cachedResults.rangeEnd
            # First case - mismatch or 2 additional queries needed - return whole query range
            if cachedEnd < queryStart or cachedStart > queryEnd or (cachedStart > queryStart and cachedEnd < queryEnd):
                return (queryDSL.range, "full")
            # Second case - extra time needed at the start
            elif cachedResults.rangeStart - cachedResults.aggInterval > queryDSL.range.start and cachedResults.rangeEnd >= queryDSL.range.end:
                return (Range(queryDSL.range.start, cachedResults.rangeStart), "start")
            # Third case - extra time needed at the end
            elif cachedResults.rangeEnd + cachedResults.aggInterval < queryDSL.range.end and cachedResults.rangeStart <= queryDSL.range.start:
                return (Range(cachedResults.rangeEnd, queryDSL.range.end), "end")
            # Fourth case - results fully encapsulate query range
            else:
                return None
        else:
            return (queryDSL.range, "full")
            
    def checkGrouping(self, queryDSL: InfluxQueryBuilder, cachedResults: TSCachev3.Series) -> TSCachev3.Series:
        if cachedResults is None:
            return None
        if set(queryDSL.groupKeys).issubset(cachedResults.groupKeys):
            return cachedResults
        else:
            return None
        
    def checkAggWindow(self, queryDSL: InfluxQueryBuilder, cachedResults: TSCachev3.Series) -> TSCachev3.Series:
        if cachedResults is None:
            return None
        if queryDSL.aggregate.getTimeWindowSeconds() >= cachedResults.aggInterval and queryDSL.aggregate.getTimeWindowSeconds() % cachedResults.aggInterval == 0:
            if cachedResults.aggFn != "median":
                return cachedResults
            else:
                if queryDSL.aggregate.getTimeWindowSeconds() == cachedResults.aggInterval and set(queryDSL.groupKeys) == cachedResults.groupKeys:
                    return cachedResults
                else:
                    return None
        else:
            return None
        

    
    def set(self, queryDSL: InfluxQueryBuilder, series: TSCachev3.Series):
        self.cache.set(queryDSL, series)
    
    def reset(self):
        self.cache = TSCachev3.TSCachev3()
    
    def __repr__(self):
        return self.__str__()
    
    def __str__(self):
        return f"CacheService: {self.cache}"
    
    def __eq__(self, other):
        return self.cache == other.cache
    
    def __hash__(self):
        return hash(self.cache)