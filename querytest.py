import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
import requests

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

print(query)

runs = 10
without = []
without2 = []
cached = []
for i in range(runs):
   start = time.time()
   tables1 = query_api.query_raw(query, org="Realtime").data
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
   
print(tables1.decode("utf-8"))
print(len(tables2))
print("Without cache: avg", sum(without)/len(without), "seconds")
print("Without cache, query2: avg", sum(without2)/len(without2), "seconds")
print("With cache: avg", sum(cached)/len(cached), "seconds")
