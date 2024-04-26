INFLUXDB_TOKEN="VJK1PL0-qDkTIpSgrtZ0vq4AG02OjpmOSoOa-yC0oB1x3PvZCk78In9zOAGZ0FXBNVkwoJ_yQD6YSZLx23WElA=="

import os, time
from influxdb_client_3 import InfluxDBClient3, Point
import pandas as pd


token = INFLUXDB_TOKEN
org = "Realtime Big Data"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

client = InfluxDBClient3(host=host, token=token, org=org)

currenttime = int(time.time())
previoustime = int(currenttime - 60)

# Convert to nanoseconds
currenttime_ns = currenttime * 1e9
previoustime_ns = previoustime * 1e9
print(currenttime_ns)
print(previoustime_ns)
#print(current)
query = """SELECT mean(value)
FROM "cpu_usage"
WHERE time > now() - 24h
GROUP BY time(1m), platform, host
"""

query2 = f"""SELECT median(value) as value,median(value) as value
FROM cpu_usage
WHERE platform = 'mac_os' OR platform = 'windows'
AND time < {format(currenttime_ns, '.0f')} AND time > {format(previoustime_ns, '.0f')}
GROUP BY time(5s), host,platform
"""

# Execute the query
startTime = time.time()
table2 = client.query(query=query2, database="Test", language="influxql", mode='all')
print(table2)
table = client.query(query=query, database="Test", language="influxql", mode='pandas')

latency = time.time() - startTime

#print(table)
#print(table)
# Convert to dataframe
df = table2.to_pandas()#.sort_values(by=["host", "time"])
print(df)


currentTime = time.time()
#for key, group in 
#    print(f"Series: {key}")
#    print(group)
dfs = df.groupby(['host', 'platform'])
groupData = []
for key, group in dfs:
    print(f"GroupKey: {key}")
    print(f'Group: {group}')
    groupData.append(group)
    #print(group)
    #print("\n

print(dfs)

print(pd.concat([groupData[0], groupData[3]]))
#print(dfs.groups)
groupByLatency = time.time() - currentTime
print("Query took %.2f seconds" % latency)
print("Group by took %.2f seconds" % groupByLatency)