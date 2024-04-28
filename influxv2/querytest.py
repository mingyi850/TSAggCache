import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
import requests
from queryDSL import InfluxQueryBuilder, QueryAggregation, QueryFilter
import json

token = "NVRAh0Hy9gLvSJVlIaYVRIP5MTktlqBHCOGxpgzIOHdSD-fu2vGjug5NmMcTv2QvH7BK6XG0tQvaoPXUWmuvLQ=="
org = "Realtime"
url = "http://localhost:8086"
cacheUrlRaw = "http://127.0.0.1:5000/api/queryRaw"
cacheUrlJson = "http://127.0.0.1:5000/api/query"
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

token3 = "VJK1PL0-qDkTIpSgrtZ0vq4AG02OjpmOSoOa-yC0oB1x3PvZCk78In9zOAGZ0FXBNVkwoJ_yQD6YSZLx23WElA=="
org3 = "Realtime Big Data"
host3 = "https://us-east-1-1.aws.cloud2.influxdata.com"

query_api = client.query_api()

'''
Notes:
1. It seems like mean aggregation is not really affected by size of data. In some cases Query2 can even be faster
2. Median is definitely affected by size of data.
3. Cache is still quicker than original query for both mean and median. However, this does not include any processing overhead for now.
4. We added additional query to test effect of additional short query to the cache (accounting for missing datapoints). Cache is still faster by around 3x
'''

query = """from(bucket: "Test")
  |> range(start: -10m)
  |> filter(fn: (r) => r["_measurement"] == "cpu_usage")
  |> filter(fn: (r) => r["_field"] == "value")
  |> filter(fn: (r) => r["platform"] == "mac_os" or r["platform"] == "windows")
  |> aggregateWindow(every: 1m, fn: median, createEmpty: false)
  |> yield(name: "median")"""

query2 = """from(bucket: "Test")
  |> range(start: -180m)
  |> filter(fn: (r) => r["_measurement"] == "cpu_usage")
  |> filter(fn: (r) => r["_field"] == "value")
  |> filter(fn: (r) => r["platform"] == "mac_os" or r["platform"] == "windows")
  |> aggregateWindow(every: 1m, fn: median, createEmpty: false)
  |> yield(name: "median")"""

influxQL = """
  SELECT cpu_usage FROM Test
  WHERE _measurement = 'cpu_usage' AND _field = 'value' AND platform = 'mac_os' OR platform = 'windows'
"""

query3Builder = (InfluxQueryBuilder()
             .withBucket("Test")
             .withFilter(QueryFilter("_measurement", "cpu_usage"))
             #.withFilter(QueryFilter("_field", "value"))
             .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
             .withFilter(QueryFilter("host", "host1"))
             .withAggregate(QueryAggregation("1m", "median", False))
             .withRelativeRange('10m', None)
             .withYield("median")
    )

query4Builder = (InfluxQueryBuilder()
              .withBucket("Test")
              .withFilter(QueryFilter("_measurement", "cpu_usage"))
              .withFilter(QueryFilter("_field", "value"))
              .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
              .withFilter(QueryFilter("host", "host1"))
              .withAggregate(QueryAggregation("10m", "median", False))
              .withRelativeRange('30m', None)
              .withYield("median")
      )
query3 = query3Builder.buildFluxStr()
query4 = query4Builder.buildFluxStr()
query3Json = query3Builder.buildJson()
query4Json = query4Builder.buildJson()


#print(query)
#print(query3)

runs = 10
delay = 0
without = []
without2 = []
cached = []

newQuery = """SELECT value
FROM "cpu_usage"
"""
client3 = influxdb_client.InfluxDBClient(url=host3, token=token3, org=org3)
query3Str = InfluxQueryBuilder.fromJson(query3Json).buildFluxStr()
query_api3 = client3.query_api()
res = query_api3.query(newQuery, org=org3)
print("GOT RES", res)
exit()
for i in range(runs):
   start = time.time()
   query3Str = InfluxQueryBuilder.fromJson(query3Json).buildFluxStr()
   tables1 = query_api.query(query3Str, org="Realtime")
   elapsed = time.time() - start
   without.append(elapsed)
   #query2
   start = time.time()
   query4Str = InfluxQueryBuilder.fromJson(query4Json).buildFluxStr()
   tables2 = query_api.query(query4Str, org="Realtime").to_json()
   elapsed = time.time() - start
   without2.append(elapsed)
   #cachedJson
   query4Json = query4Builder.buildJson()
   start = time.time()
   result = requests.post(cacheUrlJson, json=query4Json).json()
   elapsed = time.time() - start
   cached.append(elapsed)
   time.sleep(delay)

def getTableKeys(table):
    """Get the keys of a FluxTable."""
    reservedLabels = {"_field", "_measurement"}
    fields = [key.label for key in table.get_group_key() if (key.label[0] != '_' or key.label in reservedLabels)]
    tablekeys = [f"{k}={table.records[0][k]}" for k in fields]
    return tablekeys

#print(tables1.to_json())
#print(query3Json)
for table in tables1:
    print(table.get_group_key())
    tablekey = getTableKeys(table)
    print("Table key", tablekey)

    for record in table.records:
        print(record.get_time().timestamp(), record.get_value(), record["platform"])

#print("Cache result")
#print(result)
#print("JSON")
#print(tables1.to_json())
#print(json.loads(tables1.to_json()))
#print("COLUMNS")
#print(tables1[0].columns)
#print("RECORDS")
#print(tables1[1].records)
#print("Values")
#print(tables1.to_values(columns=['table', '_time', '_value']))
print(tables2)
print("TABLES 1", tables1)
jsonUncached = json.loads(tables2)
print("Res", result)
print("Uncached json len", len(jsonUncached))
print("Query 1 returned: ", len(tables1), " entries")
print("Uncached return ", len(tables2), " entries")
print("Cached returned", len(str(result)), " entries")
print("Without cache: avg", sum(without)/len(without), "seconds")
print("Without cache, query2: avg", sum(without2)/len(without2), "seconds")
print("With cache: avg", sum(cached)/len(cached), "seconds")

