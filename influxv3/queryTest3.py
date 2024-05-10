import os, time
from influxdb_client_3 import InfluxDBClient3, Point
import pandas as pd
from queryDSL import InfluxQueryBuilder, QueryAggregation, QueryFilter
import requests
import json
import configparser

config = configparser.ConfigParser()
config.read('cacheConfig.ini')
cacheConfig = config['cache']
port = cacheConfig.get('port', 5000)

influxConfig = config['influx']
token = influxConfig.get('token')
org = influxConfig.get('org', 'Realtime Big Data')
host = influxConfig.get('host', 'https://us-east-1-1.aws.cloud2.influxdata.com')

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

query2 = f"""
SELECT mean(value)
FROM cpu_usage
WHERE platform = 'mac_os' OR platform = 'windows'
AND time < {format(currenttime_ns, '.0f')} AND time > {format(previoustime_ns, '.0f')}
GROUP BY time(5s),
host,platform
"""

influxBuilder = (InfluxQueryBuilder()
               .withBucket("Test")
               .withMeasurements(["cpu_usage", "temperature"])
               .withTable("system_metrics")
               .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
               .withAggregate(QueryAggregation("10m", "mean", False))
               .withRelativeRange('300m', None)
               .withGroupKeys(["host", "platform"])
       )

influxBuilderDiffMeasurements = (InfluxQueryBuilder()
               .withBucket("Test")
               .withMeasurements(["cpu_usage", "memory_usage"])
               .withTable("system_metrics")
               .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
               .withAggregate(QueryAggregation("10m", "mean", False))
               .withRelativeRange('30m', None)
               .withGroupKeys(["host", "platform"])
       )

influxBuilderDiffExtendedRange = (InfluxQueryBuilder()
               .withBucket("Test")
               .withMeasurements(["cpu_usage", "temperature"])
               .withTable("system_metrics")
               .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
               .withAggregate(QueryAggregation("10m", "mean", False))
               .withRelativeRange('60m', None)
               .withGroupKeys(["host", "platform"])
       )

influxBuilderReAggregated = (InfluxQueryBuilder()
               .withBucket("Test")
               .withMeasurements(["cpu_usage", "memory_usage"])
               .withTable("system_metrics")
               .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
               .withAggregate(QueryAggregation("20m", "mean", False))
               .withRelativeRange('60m', None)
               .withGroupKeys(["platform"])
       )

influxBuilderReAggregatedDouble = (InfluxQueryBuilder()
               .withBucket("Test")
               .withMeasurements(["cpu_usage"])
               .withTable("system_metrics")
               .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
               .withAggregate(QueryAggregation("30m", "mean", False))
               .withRelativeRange('60m', None)
               .withGroupKeys(["platform"])
       )

influxBuilderReAggregatedAll = (InfluxQueryBuilder()
               .withBucket("Test")
               .withMeasurements(["cpu_usage", "memory_usage", "temperature"])
               .withTable("system_metrics")
               .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
               .withAggregate(QueryAggregation("30m", "mean", False))
               .withRelativeRange('60m', None)
               .withGroupKeys(["platform"])
       )

testQueries = [influxBuilder, influxBuilderDiffMeasurements, influxBuilderDiffExtendedRange, influxBuilderReAggregated, influxBuilderReAggregatedDouble, influxBuilderReAggregatedAll]

def runSuiteRaw(queryList, client):
    # Execute the query via client
    startTime = time.time()
    raw_times = []
    results = []
    for query in queryList:
        queryTime = time.time()
        queryStr = query.buildInfluxQlStr()
        table = client.query(query=queryStr, database="Test", language="influxql", mode='pandas')
        endTime = time.time() - queryTime
        raw_times.append(endTime)
        results.append(table)
        print(table)
    rawLatency = time.time() - startTime
    return rawLatency, raw_times

def runSuiteCache(queryList):
    # Execute the query via cache service
    cacheUrlJson = "http://127.0.0.1:5000/api/query"
    startTime = time.time()
    cached_times = []
    for query in queryList:
        queryJson = query.buildJson(doTrace = False)
        queryTime = time.time()
        tableResp = requests.post(cacheUrlJson, json=queryJson)
        tableJson = tableResp.json()
        resultData = tableJson.get("data")
        tableDf = pd.read_json(json.dumps(resultData), orient='records')
        endTime = time.time() - queryTime
        cached_times.append(endTime)
        print(tableDf)
    cacheLatency = time.time() - startTime
    return cacheLatency, cached_times

cacheLatency, cached_times = runSuiteCache(testQueries)
rawLatency, raw_times = runSuiteRaw(testQueries, client)

# Execute the query via cache service



print("Raw query took %.2f seconds"% rawLatency, raw_times)
print("Cache query took %.2f seconds" % cacheLatency, cached_times)

#reset cache
resetUrl = "http://127.0.0.1:5000/api/reset"
requests.post(resetUrl)
exit()
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



influxBuilder3 = (InfluxQueryBuilder()
               .withBucket("Test")
               .withMeasurements(["cpu_usage", "temperature"])
               .withTable("system_metrics")
               .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
               .withAggregate(QueryAggregation("1m", "mean", False))
               .withRelativeRange('300m', None)
               .withGroupKeys(["host", "platform"])
       )