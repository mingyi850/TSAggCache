INFLUXDB_TOKEN="VJK1PL0-qDkTIpSgrtZ0vq4AG02OjpmOSoOa-yC0oB1x3PvZCk78In9zOAGZ0FXBNVkwoJ_yQD6YSZLx23WElA=="

import os, time
from influxdb_client_3 import InfluxDBClient3, Point

token = INFLUXDB_TOKEN
org = "Realtime Big Data"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

client = InfluxDBClient3(host=host, token=token, org=org)

query = """SELECT mean(value)
FROM "cpu_usage"
WHERE time > now() - 24h
GROUP BY time(1m), platform, host
"""

query2 = """SELECT median(value) as value,median(value) as value
FROM cpu_usage
WHERE platform = 'mac_os' OR platform = 'windows'
AND time > now() - 30m
GROUP BY time(10m), host,platform
"""
#

# Execute the query
startTime = time.time()
table2 = client.query(query=query2, database="Test", language="influxql", mode='all')
print(table2)
table = client.query(query=query, database="Test", language="influxql", mode='all')

latency = time.time() - startTime

#print(table)
#print(table)
# Convert to dataframe
df = table2.to_pandas()#.sort_values(by=["host", "time"])
print(df)

print(df)


currentTime = time.time()
#for key, group in 
#    print(f"Series: {key}")
#    print(group)
dfs = df.groupby(['host', 'platform'])
for key, group in dfs:
    print(f"Group: {key}")
    #print(group)
    #print("\n
print(dfs.groups)
groupByTime = time.time() - currentTime
print("Query took %.2f seconds" % latency)
print("Group by took %.2f seconds" % groupByTime)