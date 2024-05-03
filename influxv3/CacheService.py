import TSCachev3
from queryDSL import InfluxQueryBuilder, Range
import copy
import time
from functools import partial
from influxdb_client_3 import InfluxDBClient3
import pandas as pd
from TSCachev3 import SeriesGroup, Series
from typing import List, Set, Dict, Tuple
import Utilsv3

INFLUXDB_TOKEN="VJK1PL0-qDkTIpSgrtZ0vq4AG02OjpmOSoOa-yC0oB1x3PvZCk78In9zOAGZ0FXBNVkwoJ_yQD6YSZLx23WElA==" #TODO: Remove or swap
token = INFLUXDB_TOKEN
org = "Realtime Big Data"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

class CacheService:
    def __init__(self):
        self.cache = TSCachev3.TSCachev3()
        self.client = InfluxDBClient3(host=host, token=token, org=org)
    
    def query(self, json, doTrace=False) -> Tuple[pd.DataFrame, Dict]:
        traceDict = dict()
        withTrace = partial(Utilsv3.withTrace, doTrace, traceDict)
        queryDSL, cachedResults, additionalRange = withTrace("CheckCache", lambda: self.findCacheMatch(json))
        if additionalRange is not None:
            (newRange, rangeType) = additionalRange
            newQueryDSL, results = withTrace("InfluxQuery", lambda: self.queryAdditionalRange(json, newRange, cachedResults))
            withTrace("CombineAndSet", lambda: self.setCombinedResults(newQueryDSL, cachedResults, results, rangeType))
        result = withTrace("ReconstructResult", lambda: self.reConstructResult(queryDSL))
        return result, traceDict
    
    def reConstructResult(self, queryDSL: InfluxQueryBuilder):
        newCachedResults = self.cache.get(queryDSL)
        return (newCachedResults
                  .getSlicedSeries(queryDSL.range.start, queryDSL.range.end) #Slice by time range
                  .regroup(queryDSL.groupKeys, queryDSL.measurements) # regroup based on new groupings
                  .reAggregate(queryDSL.aggregate.getTimeWindowSeconds(), queryDSL.measurements) # downsample based on new aggInterval 
                  .filterMeasurements(queryDSL.measurements) # return only required columns
                  .getCombinedDataFrame()) #combine dataframe into one
    
    def setCombinedResults(self, newQueryDSL: InfluxQueryBuilder, cachedResults: TSCachev3.Series, newResults: pd.DataFrame, rangeType: str) -> None:
        newSeries = self.combineResults(newQueryDSL, cachedResults, newResults, rangeType)
        self.cache.set(newQueryDSL, newSeries)
    
    def queryAdditionalRange(self, json, newRange: Range, cachedResults: Series) -> Tuple[InfluxQueryBuilder, pd.DataFrame]:
        newQueryDSL = InfluxQueryBuilder.fromJson(json)
        newQueryDSL = self.modifyQuery(newQueryDSL, cachedResults, newRange)
        #print("New Query", newQueryDSL.buildJson())
        query = newQueryDSL.buildInfluxQlStr()
        influxResults = self.client.query(query=query, database=newQueryDSL.bucket, language="influxql", mode='pandas')
        return newQueryDSL, influxResults
    
    def findCacheMatch(self, json) -> Tuple[InfluxQueryBuilder, TSCachev3.Series]:
        queryDSL = InfluxQueryBuilder.fromJson(json) #Convert to query builder object
        cachedResults = self.cache.get(queryDSL) #Check if a matching entry exists in cache based on table_aggFn_filter
        cachedResults = self.checkGrouping(queryDSL, cachedResults)
        cachedResults = self.checkAggWindow(queryDSL, cachedResults)
        #print("Got initial cache hit", cachedResults)
        additionalRange = self.checkAdditionalRange(queryDSL, cachedResults)
        #print(cachedResults, additionalRange)
        return queryDSL, cachedResults, additionalRange 
    
    def groupDataDict(self, data: pd.DataFrame, groupKeys: list) -> dict:
        result = dict()
        if data.empty:
            return result
        #print("Group keys are", groupKeys)
        #print("New data is", data)
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
            aggInterval = cachedResults.aggInterval
            # First case - mismatch or 2 additional queries needed - return whole query range
            if cachedEnd < queryStart or cachedStart > queryEnd or (cachedStart - aggInterval > queryStart and cachedEnd + aggInterval < queryEnd):
                return (queryDSL.range, "full")
            # Second case - extra time needed at the start
            elif cachedStart - aggInterval > queryStart and cachedEnd >= queryEnd:
                return (Range(queryStart, cachedStart), "start")
            # Third case - extra time needed at the end
            elif cachedEnd + aggInterval < queryEnd and cachedStart <= queryStart:
                return (Range(cachedEnd, queryEnd), "end")
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