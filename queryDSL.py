
from typing import List, Iterator, Optional
import time 

class BaseQueryFilter:
    def toString(self):
        pass
    
    def toJson(self):
        pass

class QueryFilter(BaseQueryFilter):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def toString (self):
        return f'filter(fn: (r) => r["{self.key}"] == "{self.value}")'
    
    def toJson(self):
        return {
            "key": self.key,
            "value": self.value
        }

    def OR(self, other: 'QueryFilter'):
        return OrQueryFilter(self, other)
    
    def AND(self, other: 'QueryFilter'):
        return AndQueryFilter(self, other)
    
    
class OrQueryFilter(BaseQueryFilter):
    def __init__(self, queryFilter1: BaseQueryFilter, queryFilter2: BaseQueryFilter):
        self.filters = [queryFilter1, queryFilter2]

    def OR(self, other: 'QueryFilter'):
        self.filters.append(other)
        return self
    
    def toString(self):
        filterFn = " or ".join([f'r["{f.key}"] == "{f.value}"' for f in self.filters])
        return f'filter(fn: (r) => {filterFn})'
    
    
    
    def toJson(self):
        return {
            "filters": [f.toJson() for f in self.filters],
            "type": "or"
        }
    
class AndQueryFilter(BaseQueryFilter):
    def __init__(self, queryFilter1: BaseQueryFilter, queryFilter2: BaseQueryFilter):
        self.filters = [queryFilter1, queryFilter2]

    def AND(self, other: 'QueryFilter'):
        self.filters.append(other)
        return self
    
    def toString(self):
        filterFn = " and ".join(['r["{f.key}"] == "{f.value}"' for f in self.filters])
        return f'filter(fn: (r) => {filterFn})'
    
    def toJson(self):
        return {
            "filters": [f.toJson() for f in self.filters],
            "type": "and"
        }
    
class QueryAggregation:
    def __init__(self, timeWindow: str, aggFunc: str, createEmpty: bool):
        self.timeWindow = timeWindow
        self.aggFunc = aggFunc
        self.createEmpty = createEmpty

    def toString(self):
        return f'aggregateWindow(every: {self.timeWindow}, fn: {self.aggFunc}, createEmpty: {str(self.createEmpty).lower()})'
    
    def toJson(self):
        return {
            "timeWindow": self.timeWindow,
            "aggFunc": self.aggFunc,
            "createEmpty": self.createEmpty
        }

class Range:
    def __init__(self, start: int, end: int):
        self.start = start
        self.end = end

    def toString(self):
        if self.end is not None:
            return f'range(start: {self.start}, stop: {self.end})'
        else:
            return f'range(start: {self.start})'
        
    def toJson(self):
        return {
            "start": self.start,
            "end": self.end
        }
    
class InfluxQueryBuilder:
    def __init__(self):
        self.range = ""
        self.bucket = ""
        self.filters = []
        self.aggregate = None
        yield_name = ""

    def withBucket(self, bucket: str) -> 'InfluxQueryBuilder':
        self.bucket = bucket
        return self

    def withFilter(self, filter: BaseQueryFilter) -> 'InfluxQueryBuilder':
        self.filters.append(filter)
        return self
    
    def withAggregate(self, aggregate: str) -> 'InfluxQueryBuilder':
        self.aggregate = aggregate
        return self
    
    def withRange(self, start: str, end: Optional[str]) -> 'InfluxQueryBuilder':
        if end is None:
            end = int(time.time())
        self.range = Range(int(start), int(end))
        return self
    
    def withRelativeRange(self, fr: str, to: Optional[str]) -> 'InfluxQueryBuilder':
        now = int(time.time())
        start = now - self._parseTime(fr)
        if to is not None:
            end = now - self._parseTime(to)
        else:
            end = now
        self.range = Range(start, end)
        return self
    
    def _parseTime(self, time: str) -> int:
        unit = time[-1]
        remaining = int(time[:-1])
        print("unit", unit)
        print("remaining", remaining)
        result = remaining * self.getUnitConversion(unit)
        print("result", result)
        return result

    def getUnitConversion(self, unit) -> int:
        if unit == "s":
            return 1
        elif unit == "m":
            return 60
        elif unit == "h":
            return 3600
        elif unit == "d":
            return 86400
        elif unit == "w":
            return 604800
        elif unit == "y":
            return 31536000
        else:
            return 0

    
    def withYield(self, name: str) -> 'InfluxQueryBuilder':
        self.yield_name = name
        return self
    
    def build(self):
        assert self.bucket != "", "Bucket is required"
        assert self.range != "", "Range is required"
        queryString = ""
        queryString += f'from(bucket: "{self.bucket}")\n'
        queryString += "|> " + self.range.toString() + "\n"
        for f in self.filters:
            queryString += "|> " + f.toString() + "\n"
        if self.aggregate is not None:
            queryString += "|> " + self.aggregate.toString() + "\n"
        if self.yield_name != "":
            queryString += "|> yield(name: " + f'"{self.yield_name}"' + ")"
        return queryString
    
    def buildJson(self):
        assert self.bucket != "", "Bucket is required"
        assert self.range != "", "Range is required"
        queryJson = {
            "bucket": self.bucket,
            "range": self.range.toJson(),
            "filters": [f.toJson() for f in self.filters],
            "yield": self.yield_name
        }
        if self.aggregate is not None:
            queryJson["aggregate"] = self.aggregate.toJson()
        return queryJson


if __name__ == "__main__":
    builder = (InfluxQueryBuilder()
             .withBucket("Test")
             .withFilter(QueryFilter("_measurement", "cpu_usage"))
             .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
             .withAggregate(QueryAggregation("1m", "median", False))
             .withRelativeRange('10m', None)
             .withYield("median")
    )

    queryStr = builder.build()
    queryJson = builder.buildJson()
    
    print(queryStr)
    print(queryJson)
    
