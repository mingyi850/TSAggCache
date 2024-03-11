from influxdb_client import QueryApi, FluxResponse
from influxdb_client.client.flux_table import FluxTable, TableList
from datetime import datetime
import copy

def getFluxTableSlice(table: FluxTable, startIdx: int, endIdx: int) -> FluxTable:
    """Get a slice of a FluxTable."""
    newTable = FluxTable()
    newTable.columns = table.columns
    newTable.records = table.records[startIdx:endIdx]
    return newTable

def getTableListSliced(tableList: TableList, startIdx: int, endIdx: int) -> TableList:
    """Get a slice of a TableList."""
    return TableList([getFluxTableSlice(table, startIdx, endIdx) for table in tableList])

def getStartTime(tableList: TableList) -> datetime:
    """Get the start time of a TableList."""
    return tableList[0].records[0].get_time()

def getEndTime(tableList: TableList) -> datetime:
    """Get the end time of a TableList."""
    return tableList[0].records[-1].get_time()

def getSecondEndTime(tableList: TableList) -> datetime:
    """Get the end time of a TableList in seconds."""
    if len(tableList[0].records) > 1:
        return tableList[0].records[-2].get_time()
    else:
        return tableList[0].records[-1].get_time()

def toTimestamp(dt: datetime) -> int:
    """Convert a datetime to a Unix timestamp."""
    return int(dt.timestamp())

def combineTableLists(original: TableList, new: TableList, appendStart: bool) -> TableList:
    """Combine a list of TableLists into a single TableList."""
    for i in range(len(original)):
        if not appendStart:
            original[i].records.extend(new[i].records)
        else:
            original[i].records = new[i].records + original[i].records
    return original

def fromJson(json: dict) -> TableList:
    """Deserialize a TableList from a JSON object."""
    tableList = TableList()
    for table in json:
        fluxTable = FluxTable()
        fluxTable.columns = table["columns"]
        for record in table["records"]:
            fluxTable.records.append(FluxRecord(record))
        tableList.append(fluxTable)
    return tableList

#Experimental feature - can turn off
def fillMissingData(tableList: TableList, start: int, end: int, aggWindow: int) -> TableList:
    """Fill missing data in a TableList."""
    for table in tableList:
        firstRecord = table.records[0]
        firstRecordTime = toTimestamp(firstRecord.get_time())
        timeDiff = firstRecordTime - start
        if timeDiff > aggWindow:
            numEntries = timeDiff // aggWindow + 1
            print("Filling records from start", numEntries, firstRecordTime, start, aggWindow)
            newRecords = []
            for i in range(numEntries, 0, -1):
                newRecord = copy.copy(firstRecord)
                newRecord["_time"] = datetime.fromtimestamp(firstRecordTime - i * aggWindow)
                newRecord["_value"] = None
                newRecords.append(newRecord)
            table.records = newRecords + table.records

        lastRecord = table.records[-1]
        lastRecordTime = toTimestamp(lastRecord.get_time())
        timeDiff = end - lastRecordTime
        if timeDiff > aggWindow:
            numEntries = timeDiff // aggWindow + 1
            newRecords = []
            for i in range(1, numEntries):
                newRecord = copy.copy(lastRecord)
                newRecord["_time"] = datetime.fromtimestamp(lastRecordTime + i * aggWindow)
                newRecord["_value"] = None
                newRecords.append(newRecord)
            table.records.extend(newRecords)
    return tableList
    
    
#TODO: Add deserialization logic for TableList and FluxTable for comparison to check correctness