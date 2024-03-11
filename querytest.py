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

query3Builder = (InfluxQueryBuilder()
             .withBucket("Test")
             .withFilter(QueryFilter("_measurement", "cpu_usage"))
             .withFilter(QueryFilter("_field", "value"))
             .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
             .withAggregate(QueryAggregation("1m", "median", False))
             .withRelativeRange('10m', None)
             .withYield("median")
    )

query4Builder = (InfluxQueryBuilder()
              .withBucket("Test")
              .withFilter(QueryFilter("_measurement", "cpu_usage"))
              .withFilter(QueryFilter("_field", "value"))
              .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
              .withAggregate(QueryAggregation("1m", "mean", False))
              .withRelativeRange('180m', None)
              .withYield("median")
      )
query3 = query3Builder.build()
query4 = query4Builder.build()
query3Json = query3Builder.buildJson()
query4Json = query4Builder.buildJson()

print(query)
print(query3)

runs = 10
delay = 0
without = []
without2 = []
cached = []
for i in range(runs):
   start = time.time()
   query3Str = InfluxQueryBuilder.fromJson(query3Json).build()
   tables1 = query_api.query(query3Str, org="Realtime")
   elapsed = time.time() - start
   without.append(elapsed)
   #query2
   start = time.time()
   query4Str = InfluxQueryBuilder.fromJson(query4Json).build()
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

#print(tables1.to_json())
print(query3Json)
for table in tables1:
    for record in table.records:
        print(record.get_time().timestamp(), record.get_value())

#print("Cache result")
#print(result)
print("JSON")
print(tables1.to_json())
print(json.loads(tables1.to_json()))
print("COLUMNS")
print(tables1[0].columns)
print("RECORDS")
print(tables1[1].records)
#print("Values")
#print(tables1.to_values(columns=['table', '_time', '_value']))
print(len(tables1))
print(len(tables2))
print(len(result))
print("Without cache: avg", sum(without)/len(without), "seconds")
print("Without cache, query2: avg", sum(without2)/len(without2), "seconds")
print("With cache: avg", sum(cached)/len(cached), "seconds")

#Plots 
import matplotlib.pyplot as plt
