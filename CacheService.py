import TSCachev3
from queryDSL import InfluxQueryBuilder

class CacheService:
    def __init__(self):
        self.cache = TSCachev3.TSCachev3()
    
    def query(self, json):
        queryDSL = InfluxQueryBuilder.InfluxQueryBuilder.fromJson(json)
        cachedResults = self.cache.get(queryDSL)
        # Check that grouping is subset of cached group
        # Check that aggWindow is of higher granularity than search aggWindow
            # Check that aggFn is compatible
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