from __future__ import annotations
from datetime import datetime
from math import ceil
from queryDSL import InfluxQueryBuilder
from queryDSL import AndQueryFilter
import pandas as pd
from typing import Set, List, Dict

''' TO Fetch series
- use table key
- check grouping fits
- check aggFn fits
'''

essentialColumns = set(["time", "iox::measurement"])

class SeriesGroup:
    def __init__(self, groupKeys: Dict, data: pd.DataFrame):
        self.groupKeys = groupKeys
        self.data = data
        self.essentialColumns = essentialColumns.union(set(groupKeys.keys()))
        self.variableColumns = set([col for col in data.columns if col not in self.essentialColumns])

    def getSlice(self, start: int, end: int) -> SeriesGroup:
        return SeriesGroup(self.groupKeys, self.data[start:end])

    def dropColumns(self, columns: List[str]):
        return SeriesGroup(self.groupKeys, self.data.drop(columns, axis=1, inplace=False))
    
    def retainColumns(self, columns: List[str]):
        unwantedColumns = list(self.variableColumns.difference(set(columns)))
        return self.dropColumns(unwantedColumns)
    
    @staticmethod
    def mergeSeriesGroups(seriesGroups: List[SeriesGroup], aggFn: str, measurements: List[str], groupings: Dict[str, str]) -> SeriesGroup:
        fnDict = {
            "mean": pd.DataFrame.mean,
            "sum": pd.DataFrame.sum,
            "max": pd.DataFrame.max,
            "min": pd.DataFrame.min,
            "median": pd.DataFrame.median,
            "count": pd.DataFrame.count,
            "first": pd.DataFrame.first,
            "last": pd.DataFrame.last
        }
        if len(seriesGroups) == 1:
            return SeriesGroup(groupings, seriesGroups[0].data)
        else:
            dfs = [seriesGroup.data for seriesGroup in seriesGroups]
            for df in dfs:
                df.reset_index(drop=True, inplace=True)
            measurements = [f"{aggFn}_{measurement}" for measurement in measurements]
            concatedMeasurements = {measurement: pd.concat([df[measurement] for df in dfs], axis=1) for measurement in measurements}
            #print("Concatenated", concatedMeasurements)
            combinedMeasurements = {measurement: fnDict[aggFn](concatedMeasurements[measurement], axis=1) for measurement in measurements}
            #print("Combined", combinedMeasurements)
            newDf = dfs[0].copy()
            for measurement in measurements:
                newDf[measurement] = combinedMeasurements[measurement]
            #print("New df", newDf)
            return SeriesGroup(groupings, newDf)
    
    def reAggregate(self, newAggWindow: int, measurements: List[str], aggFn: str):
        measurements = [f"{aggFn}_{measurement}" for measurement in measurements]
        measurementsColumns = [self.data[[measurement]] for measurement in measurements]
        #print("Original df", self.data)
        timeDf = self.data.set_index('time', inplace=False)
        aggDict = {**{measurement: aggFn for measurement in measurements}, **{colName: 'first' for colName in self.essentialColumns if colName != 'time'}}
        downsampled = timeDf.resample(f'{newAggWindow}S').agg(aggDict)
        #print("Downsampled", downsampled)
        columns = [*["time", "iox::measurement"], *self.groupKeys.keys(), *measurements]
        print(columns)
        downsampled = downsampled.reset_index()[columns]
        #print("Downsampled reordered", downsampled)
        return SeriesGroup(self.groupKeys, downsampled) 

        
class Series:
    def __init__(self, table: str, groups: Set[str], aggFn: str, aggInterval: int, rangeStart: int, rangeEnd: int, data: Dict[tuple, SeriesGroup]):
        self.table = table
        self.groupKeys = groups
        self.aggFn = aggFn
        self.aggInterval = aggInterval
        self.rangeStart = rangeStart
        self.rangeEnd = rangeEnd 
        self.data = data # A Dict of seriesGroup objects, with keys as tuples of group keys

    def __str__(self):
        return f"Table: {self.table}, Group: {self.groupKeys}, AggFn: {self.aggFn}, Data: {self.data} "
    
    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.tableKey == other.tableKey and self.groupKeys == other.groupKeys and self.aggFn == other.aggFn

    def __hash__(self):
        return hash((self.tableKey, self.groupKeys, self.aggFn))

    def setData(self, data):
        self.data = data

    def getData(self):
        return self.data

    def getTableKey(self):
        return self.tableKey

    def getGroupKeys(self):
        return self.groupKeys

    def getAggFn(self):
        return self.aggFn
    
    def getSlicedSeries(self, rangeStart: int, rangeEnd: int) -> Series:
        startIndex = int((rangeStart - self.rangeStart) / self.aggInterval)
        endIndex = ceil((rangeEnd - self.rangeStart) / self.aggInterval) + 1
        result = dict()
        for key in self.data.keys():
            seriesGroup = self.data[key]
            result[key] = seriesGroup.getSlice(startIndex, endIndex)
        return Series(self.table, self.groupKeys, self.aggFn, self.aggInterval, rangeStart, rangeEnd, result)
    
    def getCombinedDataFrame(self) -> pd.DataFrame:
        if len(self.data.values()) == 0:
            return pd.DataFrame()
        result = pd.concat([seriesGroup.data for seriesGroup in self.data.values()])
        return result
    
    def filterMeasurements(self, measurements: List[str]):
        augmentedMeasurements = [f"{self.aggFn}_{measurement}" for measurement in measurements]
        result = dict()
        for key in self.data.keys():
            seriesGroup = self.data[key]
            result[key] = seriesGroup.retainColumns(augmentedMeasurements)
        return Series(self.table, self.groupKeys, self.aggFn, self.aggInterval, self.rangeStart, self.rangeEnd, result)
    
    def regroup(self, newGroups: List[str], measurements: List[str]) -> Series:
        if set(newGroups) == self.groupKeys:
            return Series(self.table, self.groupKeys, self.aggFn, self.aggInterval, self.rangeStart, self.rangeEnd, self.data)
        newGroupings = dict()
        sortedGroups = sorted(newGroups)
        for seriesGroup in self.data.values():
            newGroupKey = tuple([seriesGroup.groupKeys[key] for key in sortedGroups])
            if newGroupKey in newGroupings:
                newGroupings[newGroupKey].append(seriesGroup)
            else:
                newGroupings[newGroupKey] = [seriesGroup]
        for group in newGroupings:
            groupKey = dict(zip(sortedGroups, group))
            newGroupings[group] = SeriesGroup.mergeSeriesGroups(newGroupings[group], self.aggFn, measurements, groupKey)
        return Series(self.table, newGroups, self.aggFn, self.aggInterval, self.rangeStart, self.rangeEnd, newGroupings)
    
    def reAggregate(self, newAggWindow: int, measurements: List[str]) -> Series:
        if newAggWindow == self.aggInterval:
            return Series(self.table, self.groupKeys, self.aggFn, self.aggInterval, self.rangeStart, self.rangeEnd, self.data)
        newData = dict()
        for key in self.data:
            seriesGroup = self.data[key]
            newData[key] = seriesGroup.reAggregate(newAggWindow, measurements, self.aggFn)
        return Series(self.table, self.groupKeys, self.aggFn, newAggWindow, self.rangeStart, self.rangeEnd, newData)

        
    
def getTableKey(queryDSL: InfluxQueryBuilder) -> str:
    return queryDSL.table

def getAggFn(queryDSL: InfluxQueryBuilder) -> str:
    return queryDSL.aggregate.aggFunc

def getFiltersKey(queryDSL: InfluxQueryBuilder) -> str:
    return AndQueryFilter(queryDSL.filters).asSetKey()

def getGroupKeys(queryDSL: InfluxQueryBuilder):
    return set(queryDSL.groupKeys)

def getSeriesKey(queryDSL: InfluxQueryBuilder):
    return f"{getTableKey(queryDSL)}_{getAggFn(queryDSL)}_{getFiltersKey(queryDSL)}"

class TSCachev3:
    def __init__(self):
        self.cache = {}
        
    def get(self, queryDSL: InfluxQueryBuilder) -> Series:
        seriesKey = getSeriesKey(queryDSL)
        if seriesKey in self.cache:
            return self.cache[seriesKey]
        else:
            return None
    
    def set(self, queryDSL: InfluxQueryBuilder, series: Series):
        seriesKey = getSeriesKey(queryDSL)
        self.cache[seriesKey] = series





        
    