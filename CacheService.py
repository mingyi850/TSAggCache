import TSCachev3
from queryDSL import InfluxQueryBuilder, Range
import copy
from influxdb_client_3 import InfluxDBClient3

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

        # OK we now have good - need to slice
        # - First slice by time range. IF we need extra time, we get extra time by making entire query again.
        # Set the cache with the combined results 

        # Now we want to give user back what they want
        # - First we slice by time range they need
        # - Second we slice by columns they are looking for
        # - Third we regroup columns based on new groupings
        # - Lastly we re-aggregate results

        #Return the modified value
        return cachedResults

    # Manipulate existing series:
    # - if previous series was none, create new series
    # - if additional range was original range (no overlap or did not exist), append series
    # - if additional range was at the start, append series and update range start
    # - if additional range was at the end, append series and update range end 
    def appendSeries(self, queryDSL: InfluxQueryBuilder, series: TSCachev3.Series):
        if series is not None:
            series.data = series.data.append(series.data)
            series.rangeStart = min(series.rangeStart, series.rangeStart)
            series.rangeEnd = max(series.rangeEnd, series.rangeEnd)
            self.cache.set(queryDSL, series)
    
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