import TSCachev3
from queryDSL import InfluxQueryBuilder, Range
import copy
from influxdb_client_3 import InfluxDBClient3
import pandas as pd

INFLUXDB_TOKEN="VJK1PL0-qDkTIpSgrtZ0vq4AG02OjpmOSoOa-yC0oB1x3PvZCk78In9zOAGZ0FXBNVkwoJ_yQD6YSZLx23WElA==" #TODO: Remove or swap
token = INFLUXDB_TOKEN
org = "Realtime Big Data"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

class CacheService:
    def __init__(self):
        self.cache = TSCachev3.TSCachev3()
        self.client = InfluxDBClient3(host=host, token=token, org=org)

    def query(self, json):
        queryDSL = InfluxQueryBuilder.fromJson(json)
        cachedResults = self.cache.get(queryDSL)
        cachedResults = self.checkGrouping(queryDSL, cachedResults)
        cachedResults = self.checkAggWindow(queryDSL, cachedResults)
        additionalRange = self.checkAdditionalRange(queryDSL, cachedResults)
        if additionalRange is not None:
            (newRange, rangeType) = additionalRange
            newQueryDSL = InfluxQueryBuilder.fromJson(json)
            newQueryDSL = self.modifyQuery(newQueryDSL, cachedResults, newRange)
            query = newQueryDSL.buildInfluxQlString()
            results = self.client.query(query=query, database=queryDSL.bucket, language="influxql", mode='pandas')
            newSeries = self.combineResults(self, newQueryDSL, cachedResults, results, rangeType)
            self.cache.set(newQueryDSL, newSeries)
        
        # Now we want to give user back what they want
        # - First we slice by time range they need
        # - Second we slice by columns they are looking for
        # - Third we regroup columns based on new groupings
        # - Lastly we re-aggregate results

        #Return the modified value
        return cachedResults

    def groupDataDict(self, data: pd.DataFrame, groupKeys: list) -> dict:
        result = dict()
        grouped = data.groupby(groupKeys)
        for key, group in grouped:
            seriesKeyDict = dict()
            for i in range(len(groupKeys)):
                seriesKeyDict[groupKeys[i]] = key[i]
            seriesGroup = TSCachev3.SeriesGroup(seriesKeyDict, group)
            result[key] = seriesGroup
        return result
    # Manipulate existing series:
    # - if previous series was none, create new series
    # - if additional range was original range (no overlap or did not exist), append series
    # - if additional range was at the start, append series and update range start
    # - if additional range was at the end, append series and update range end 
    def combineResults(self, newQueryDSL: InfluxQueryBuilder, cachedSeries: TSCachev3.Series, newResults: pd.DataFrame, rangeType: str) -> TSCachev3.Series:
        if rangeType == "full":
            newSeriesData = self.groupDataDict(newResults, newQueryDSL.groupKeys)
            newSeries = TSCachev3.Series(set(newQueryDSL.groupKeys), newQueryDSL.aggregate.aggFunc, newQueryDSL.aggregate.timeWindow, newQueryDSL.range.start, newQueryDSL.range.end, newSeriesData)
            return newSeries
        elif rangeType == "start":
            newSeriesData = self.groupDataDict(newResults, newQueryDSL.groupKeys)
            existing = cachedSeries.getData()
            for key, value in existing:
                newSeriesData[key] = pd.concat([newSeriesData[key], value])
            newSeries = TSCachev3.Series(set(newQueryDSL.groupKeys), newQueryDSL.aggregate.aggFunc, newQueryDSL.aggregate.timeWindow, newQueryDSL.range.start, cachedSeries.rangeEnd, newSeriesData)
            return newSeries
        elif rangeType == "end":
            newSeriesData = self.groupDataDict(newResults, newQueryDSL.groupKeys)
            existing = cachedSeries.getData()
            for key, value in existing:
                newSeriesData[key] = pd.concat([value, newSeriesData[key]])
            newSeries = TSCachev3.Series(set(newQueryDSL.groupKeys), newQueryDSL.aggregate.aggFunc, newQueryDSL.aggregate.timeWindow, cachedSeries.rangeStart, newQueryDSL.range.end, newSeriesData)
            return newSeries
        

    
    def modifyQuery(self, queryDSL: InfluxQueryBuilder, cachedResults: TSCachev3.Series, newRange: Range) -> InfluxQueryBuilder:
        queryDSL.range = newRange
        queryDSL.measurements = ['*']
        if cachedResults is not None:
            queryDSL.groupKeys = list(cachedResults.groupKeys)
            queryDSL.aggregate.aggWindow = cachedResults.aggWindow
            queryDSL.aggregate.aggFunc = cachedResults.aggFn
        return queryDSL
    
    def checkAdditionalRange(self, queryDSL: InfluxQueryBuilder, cachedResults: TSCachev3.Series) -> Range:
        if cachedResults is not None:
            # First case - mismatch or 2 additional queries needed - return whole query range
            if cachedResults.rangeEnd < queryDSL.range.start or cachedResults.rangeStart > queryDSL.range.end or (cachedResults.rangeStart > queryDSL.range.start and cachedResults.rangeEnd < queryDSL.range.end):
                return (queryDSL.range, "full")
            # Second case - extra time needed at the start
            elif cachedResults.rangeStart > queryDSL.range.start and cachedResults.rangeEnd >= queryDSL.range.end:
                return (Range(queryDSL.range.start, cachedResults.rangeStart), "start")
            # Third case - extra time needed at the end
            elif cachedResults.rangeEnd < queryDSL.range.end and cachedResults.rangeStart <= queryDSL.range.start:
                return (Range(cachedResults.rangeEnd, queryDSL.range.end), "end")
            # Fourth case - results fully encapsulate query range
            else:
                return None
        else:
            return queryDSL.range
            
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
        if queryDSL.aggregate.aggWindow >= cachedResults.aggWindow and queryDSL.aggregate.aggWindow % cachedResults.aggWindow == 0:
            if cachedResults.aggFn != "median":
                return cachedResults
            else:
                if queryDSL.aggregate.aggWindow == cachedResults.aggWindow:
                    return cachedResults
                else:
                    return None
        else:
            return None
        

    
    def set(self, queryDSL: InfluxQueryBuilder.InfluxQueryBuilder, series: TSCachev3.Series):
        self.cache.set(queryDSL, series)
    
    def clear(self):
        self.cache = TSCachev3.TSCachev3()
    
    def __repr__(self):
        return self.__str__()
    
    def __str__(self):
        return f"CacheService: {self.cache}"
    
    def __eq__(self, other):
        return self.cache == other.cache
    
    def __hash__(self):
        return hash(self.cache)