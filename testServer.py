import json
from flask import Flask, jsonify, request
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision

token = "NVRAh0Hy9gLvSJVlIaYVRIP5MTktlqBHCOGxpgzIOHdSD-fu2vGjug5NmMcTv2QvH7BK6XG0tQvaoPXUWmuvLQ=="
org = "Realtime"
url = "http://localhost:8086"

"""
TODO: 
1. Think of datastore to efficiently retrieve data from cache. Should be able to take slice of previous data easily.
    a. Arrays? With each index corresponding to a datapoint
        i. Then we need to find way to index the array based on time. 
        ii. for each entry received from previous data, we store in array and store the index of each entry.
        iii. Client returns data in CSV format.  
    b. Pandas dataframe?
"""
testShortQuery = """from(bucket: "Test")
  |> range(start: -10m)
  |> filter(fn: (r) => r["_measurement"] == "cpu_usage")
  |> filter(fn: (r) => r["_field"] == "value")
  |> filter(fn: (r) => r["platform"] == "mac_os" or r["platform"] == "windows")
  |> aggregateWindow(every: 1m, fn: median, createEmpty: false)
  |> yield(name: "median")"""

client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

app = Flask('queryServer')
app.request_class.charset = None

queryCache = dict()

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"})

@app.route('/api/query', methods=['POST'])
def query():
    data = (request.data)
    query = data.decode("utf-8")
    print(query)
    #query = data['query']
    if query in queryCache:
        #print("Fetching from cache")
        response = query_api.query_raw(testShortQuery, org="Realtime")        
        return queryCache[query]
    else:
        #print("Fetching from DB")
        response = query_api.query_raw(query, org="Realtime")
        tables = query_api._to_tables(response, query_options=query_api._get_query_options())
        print(len(tables))
        #for table in tables:
            #print(table.records)
        queryCache[query] = response
        return response
            


if __name__ == '__main__':
    app.run(debug=True, port='5000')