import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
import requests
from queryDSL import InfluxQueryBuilder, QueryAggregation, QueryFilter

token = "NVRAh0Hy9gLvSJVlIaYVRIP5MTktlqBHCOGxpgzIOHdSD-fu2vGjug5NmMcTv2QvH7BK6XG0tQvaoPXUWmuvLQ=="
org = "Realtime"
url = "http://localhost:8086"
cacheUrl = "http://127.0.0.1:5000/api/query"
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

query3 = """from(bucket: "Test")
|> range(start: 1709460754, stop: 1710060754)
|> filter(fn: (r) => r["platform"] == "mac_os")
|> filter(fn: (r) => r["platform"] == "windows")
|> aggregateWindow(every: 1m, fn: median, createEmpty: False)
|> yield(name: median)"""

builder = (InfluxQueryBuilder()
             .withBucket("Test")
             .withFilter(QueryFilter("_measurement", "cpu_usage"))
             .withFilter(QueryFilter("_field", "value"))
             .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
             .withAggregate(QueryAggregation("1m", "median", False))
             .withRelativeRange('10m', None)
             .withYield("median")
    )
query3 = builder.build()

print(query)
print(query3)

runs = 10
without = []
without2 = []
cached = []
for i in range(runs):
   start = time.time()
   tables1 = query_api.query(query3, org="Realtime")
   elapsed = time.time() - start
   without.append(elapsed)
   #query2
   start = time.time()
   tables2 = query_api.query_raw(query2, org="Realtime").data
   elapsed = time.time() - start
   without2.append(elapsed)
   #cached
   start = time.time()
   result = requests.post(cacheUrl, data=query2).text
   elapsed = time.time() - start
   cached.append(elapsed)

print(tables1.to_json())
for table in tables1:
    for record in table.records:
        print(record.get_time(), record.get_value())
print(len(tables1))
print(len(tables2))
print("Without cache: avg", sum(without)/len(without), "seconds")
print("Without cache, query2: avg", sum(without2)/len(without2), "seconds")
print("With cache: avg", sum(cached)/len(cached), "seconds")
