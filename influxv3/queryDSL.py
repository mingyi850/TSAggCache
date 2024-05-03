
from typing import List, Iterator, Optional
import time 
import json

class BaseQueryFilter:
    def toString(self):
        pass
    
    def toJson(self):
        pass

    def toKey(self):
        pass

    def asSetKey(self):
        pass

    @staticmethod
    def fromJson(json):
        if "type" not in json or json["type"] == "raw":
            return QueryFilter(json["key"], json["value"])
        type = json["type"]
        if type == 'or':
            return OrQueryFilter.fromJson(json)
        elif type == 'and':
            return AndQueryFilter.fromJson(json)
        

class QueryFilter(BaseQueryFilter):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def toString (self):
        return f'filter(fn: (r) => r["{self.key}"] == "{self.value}")'
    
    def toJson(self):
        return {
            "key": self.key,
            "value": self.value,
            "type": "raw"
        }
    
    def toKey(self):
        return "raw(" + self.key + "," + self.value + ")"

    def OR(self, other: 'QueryFilter'):
        return OrQueryFilter([self, other])
    
    def AND(self, other: 'QueryFilter'):
        return AndQueryFilter([self, other])
    
    def asSetKey(self):
        return f"({self.key}={self.value})"
    
    
class OrQueryFilter(BaseQueryFilter):
    def __init__(self, filters: List[BaseQueryFilter]):
        self.filters = filters

    def OR(self, other: 'QueryFilter'):
        self.filters.append(other)
        return self
    
    def toString(self):
        filterFn = " or ".join([f'r["{f.key}"] == "{f.value}"' for f in self.filters])
        return f'filter(fn: (r) => {filterFn})'
    
    def toJson(self):
        return {
            "filter": [f.toJson() for f in self.filters],
            "type": "or"
        }
    
    @staticmethod
    def fromJson(json):
        filters = [BaseQueryFilter.fromJson(f) for f in json["filter"]]
        return OrQueryFilter(filters)
    
    def toKey(self):
        return "or(" + ",".join([f.toKey() for f in self.filters]) + ")"
    
    def asSetKey(self):
        setMembers = ",".join(sorted([f.asSetKey() for f in self.filters]))
        return f"(or: {setMembers})"
    
class AndQueryFilter(BaseQueryFilter):
    def __init__(self, filters: List[BaseQueryFilter]):
        self.filters = filters

    def AND(self, other: 'QueryFilter'):
        self.filters.append(other)
        return self
    
    def toString(self):
        filterFn = " and ".join(['r["{f.key}"] == "{f.value}"' for f in self.filters])
        return f'filter(fn: (r) => {filterFn})'
    
    def toJson(self):
        return {
            "filter": [f.toJson() for f in self.filters],
            "type": "and"
        }
    
    def toKey(self):
        return "and(" + ",".join([f.toKey() for f in self.filters]) + ")"
    
    def asSetKey(self):
        setMembers = ",".join(sorted([f.asSetKey() for f in self.filters]))
        return f"(and: {setMembers})"
    
    @staticmethod
    def fromJson(json):
        filters = [BaseQueryFilter.fromJson(f) for f in json["filter"]]
        return AndQueryFilter(filters)
    
class QueryAggregation:
    def __init__(self, timeWindow: str, aggFunc: str, createEmpty: bool = False):
        self.timeWindow = timeWindow
        self.aggFunc = aggFunc
        self.createEmpty = createEmpty

    def toFluxString(self):
        return f'aggregateWindow(every: {self.timeWindow}, fn: {self.aggFunc}, createEmpty: {str(self.createEmpty).lower()})'
    
    def toJson(self):
        return {
            "timeWindow": self.timeWindow,
            "aggFunc": self.aggFunc,
            "createEmpty": self.createEmpty
        }
    
    @staticmethod
    def fromJson(json):

        return QueryAggregation(json["timeWindow"], json["aggFunc"], json.get("createEmpty", False))
    
    def getTimeWindowSeconds(self):
        unit = self.timeWindow[-1]
        remaining = int(self.timeWindow[:-1])
        return remaining * InfluxQueryBuilder.getUnitConversion(unit)

class RelativeRange:
    def __init__(self, fr: str, to: Optional[str]):
        self.fr = fr
        self.to = to

    def toInfluxQLString(self):
        returnStr = ""
        returnStr += f'time > now() - {self.fr}'
        if self.to is not None:
            returnStr += f' AND time < now() - {self.to}'
        return returnStr
    
    def toJson(self):
        return {
            "fr": self.fr,
            "to": self.to
        }
    
    def __repr__(self) -> str:
        return f"RelativeRange(fr: {self.fr}, to: {self.to})"

    @staticmethod
    def fromJson(json):
        return RelativeRange(json["fr"], json["to"])

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
    
    def toInfluxQLString(self):
        if self.end is not None:
            return f"time > {format(toNano(self.start), '.0f')} AND time < {format(toNano(self.end), '.0f')}"
        else:
            return f"time > {format(toNano(self.start), '.0f')}"
    
    @staticmethod
    def fromJson(json):
        return Range(json["start"], json["end"])
    
class InfluxQueryBuilder:
    def __init__(self):
        self.range = None
        self.relativeRange = None
        self.measurements = [] #Required for influxQL
        self.bucket = ""
        self.filters = []
        self.aggregate = None
        self.table = None #Required for influxQL
        self.groupKeys = []
        self.yield_name = ""

    def withGroupKeys(self, keys: str) -> 'InfluxQueryBuilder':
        self.groupKeys.extend(keys)
        return self
    
    def withMeasurements(self, measurements: str) -> 'InfluxQueryBuilder':
        self.measurements.extend(measurements)
        return self

    def withBucket(self, bucket: str) -> 'InfluxQueryBuilder':
        self.bucket = bucket
        return self

    def withFilter(self, filter: BaseQueryFilter) -> 'InfluxQueryBuilder':
        self.filters.append(filter)
        return self
    
    def withTable(self, table: str) -> 'InfluxQueryBuilder':
        self.table = table
        return self
    
    def withAggregate(self, aggregate: QueryAggregation) -> 'InfluxQueryBuilder':
        self.aggregate = aggregate
        return self
    
    def withRange(self, start: str, end: Optional[str]) -> 'InfluxQueryBuilder':
        if end is None:
            end = int(time.time())
        self.range = Range(int(start), int(end))
        return self
    
    def withRelativeRange(self, fr: str, to: Optional[str]) -> 'InfluxQueryBuilder':
        self.relativeRange = RelativeRange(fr, to)
        return self
    
    def getRangeFromRelative(self) -> Range:
        now = int(time.time())
        start = now - self._parseTime(self.relativeRange.fr)
        if self.relativeRange.to is not None:
            end = now - self._parseTime(self.relativeRange.to)
        else:
            end = now
        return Range(start, end)
    
    def _parseTime(self, time: str) -> int:
        unit = time[-1]
        remaining = int(time[:-1])
        result = remaining * InfluxQueryBuilder.getUnitConversion(unit)
        return result

    @staticmethod
    def getUnitConversion(unit) -> int:
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
    
    def buildFluxStr(self):
        assert self.bucket != "", "Bucket is required"
        assert not (self.range is None and self.relativeRange is None), "Range or relative range is required"
        range = self.range if self.range is not None else self.getRangeFromRelative()
        queryString = ""
        queryString += f'from(bucket: "{self.bucket}")\n'
        queryString += "|> " + range.toString() + "\n"
        for f in self.filters:
            queryString += "|> " + f.toString() + "\n"
        if self.aggregate is not None:
            queryString += "|> " + self.aggregate.toFluxString() + "\n"
        if self.yield_name != "":
            queryString += "|> yield(name: " + f'"{self.yield_name}"' + ")"
        return queryString
    
    def getAggregateMeasurements(self):
        if self.aggregate is not None:
            if self.measurements[0] != '*':
                return ", ".join([f"{self.aggregate.aggFunc}({m}) as {self.aggregate.aggFunc}_{m}" for m in self.measurements])
            else:
                return ", ".join([f"{self.aggregate.aggFunc}({m})" for m in self.measurements])
        else:
            return ", ".join(self.measurements)
    
    def parseFilter(self, filter: BaseQueryFilter) -> str:
        if isinstance(filter, QueryFilter):
            return f"{filter.key} = '{filter.value}'"
        elif isinstance(filter, OrQueryFilter):
            return " OR ".join([self.parseFilter(f) for f in filter.filters])
        elif isinstance(filter, AndQueryFilter):
            return " AND ".join([self.parseFilter(f) for f in filter.filters])
        
    def parseFilters(self, filters: List[BaseQueryFilter]) -> str:
        return " AND ".join([self.parseFilter(f) for f in filters])
    
    def getGroupByInflux(self):
        queryStr = ""
        if self.aggregate is not None and self.groupKeys != []:
            queryStr += f'GROUP BY time({self.aggregate.timeWindow}), {",".join(self.groupKeys)}\n'
        elif self.aggregate is not None and self.groupKeys == []:
            queryStr += f'GROUP BY time({self.aggregate.timeWindow})\n'
        elif self.aggregate is None and self.groupKeys != []:
            queryStr += f'GROUP BY {",".join(self.groupKeys)}\n'
        return queryStr
    
    def buildInfluxQlStr(self):
        assert not (self.range is None and self.relativeRange is None), "Range or relative range is required"
        range = self.range if self.range is not None else self.getRangeFromRelative()
        queryString = ""
        queryString += f'SELECT {self.getAggregateMeasurements()}\n'
        queryString += f'FROM {self.table}\n'
        queryString += f'WHERE {self.parseFilters(self.filters)}\n'
        queryString += f'AND {range.toInfluxQLString()}\n'
        queryString += self.getGroupByInflux()
        return queryString
    
    def buildJson(self, doTrace=False):
        assert self.bucket != "", "Bucket is required"
        assert not (self.range is None and self.relativeRange is None), "Range or relative range is required"
        range = self.range if self.range is not None else self.getRangeFromRelative()
        
        queryJson = {
            "bucket": self.bucket,
            "range": range.toJson(),
            "relativeRange": self.relativeRange.toJson() if self.relativeRange is not None else None,
            "filters": [f.toJson() for f in self.filters],
            "yield": self.yield_name,
            "measurements": self.measurements,
            "table": self.table,
            "groupKeys": self.groupKeys,
            "doTrace": doTrace
        }
        if self.aggregate is not None:
            queryJson["aggregate"] = self.aggregate.toJson()
        return queryJson
    
    @staticmethod
    def fromJson(json) -> 'InfluxQueryBuilder':
        builder = InfluxQueryBuilder()
        if "bucket" in json:
            builder.bucket = json["bucket"]
        if "range" in json:
            builder.range = Range.fromJson(json["range"])
        if "filters" in json:
            builder.filters = [BaseQueryFilter.fromJson(f) for f in json["filters"]]
        if "aggregate" in json:
            builder.aggregate = QueryAggregation.fromJson(json["aggregate"])
        if "yield" in json:
            builder.yield_name = json["yield"]
        if "measurements" in json:
            builder.measurements = json["measurements"]
        if "table" in json:
            builder.table = json["table"]
        if "groupKeys" in json:
            builder.groupKeys = json["groupKeys"]
        if "relativeRange" in json:
            builder.relativeRange = RelativeRange.fromJson(json["relativeRange"])
        return builder

def toNano(time):
    return time * 1e9

if __name__ == "__main__":
    builder = (InfluxQueryBuilder()
             .withBucket("Test")
             .withFilter(QueryFilter("_measurement", "cpu_usage"))
             .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
             .withAggregate(QueryAggregation("1m", "median", False))
             .withRelativeRange('10m', None)
             .withYield("median")
    )

    influxBuilder = (InfluxQueryBuilder()
               .withBucket("Test")
               .withMeasurements(["value"])
               .withTable("cpu_usage")
               .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
               .withAggregate(QueryAggregation("10m", "median", False))
               .withRelativeRange('30m', None)
               .withGroupKeys(["host", "platform"])
       )

    queryStr = builder.buildFluxStr()
    queryJson = builder.buildJson()

    influxQueryStr = influxBuilder.buildInfluxQlStr()
    influxQueryJson = influxBuilder.buildJson()
    influxBuilder2 = InfluxQueryBuilder.fromJson(influxQueryJson)
    influxQueryStr2 = influxBuilder2.buildInfluxQlStr()
    
    print(influxQueryJson)
    print(influxQueryStr)
    print(influxQueryStr2)
    print(queryStr)
    print(json.dumps(queryJson))
    print(InfluxQueryBuilder.fromJson(queryJson).buildFluxStr())
    
