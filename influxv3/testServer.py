import json
from flask import Flask, jsonify, request
from queryDSL import InfluxQueryBuilder, QueryAggregation, QueryFilter
from CacheService import CacheService

token = "NVRAh0Hy9gLvSJVlIaYVRIP5MTktlqBHCOGxpgzIOHdSD-fu2vGjug5NmMcTv2QvH7BK6XG0tQvaoPXUWmuvLQ=="
org = "Realtime"
url = "http://localhost:8086"

"""
TODO: 
1. Think of datastore to efficiently retrieve data from cache. Should be able to take slice of previous data easily.
    a. Arrays? With each index corresponding to a timestamp.
        i. Then we need to find way to index the array based on time. 
        ii. for each entry received from previous data, we store in array and store the index of each entry.
        iii. Client returns data in CSV format. Need to parse this into entries
        iv. What about missing indicies which are technically in the DB? do we approximate or do we re-fetch?
        v. Binary search to find time slices? 

    b. Pandas dataframe?
"""

app = Flask('queryServer')
app.request_class.charset = None

queryCache = dict()
cacheService = CacheService()

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"})

"""
Takes in a request with a JSON body containing query parameters
Example:
{
    'bucket': 'Test',
    'range': {'start': 1713106836, 'end': 1713108636},
    'relativeRange': {'fr': '30m', 'to': None},
    'filters': [{'filter': [{'key': 'platform', 'value': 'mac_os', 'type': 'raw'}, {'key': 'platform', 'value': 'windows', 'type': 'raw'}], 'type': 'or'}],
    'yield': '',
    'measurements': ['value'],
    'table': 'cpu_usage',
    'groupKeys': ['host', 'platform'],
    'aggregate': {'timeWindow': '10m', 'aggFunc': 'median', 'createEmpty': False}
}
Returns a json containing Table data from influxDB
"""
@app.route('/api/query', methods=['POST'])
def query():
    #print(request.data.decode('utf-8'))
    #print(request.data)
    requestJson = json.loads(request.data)
    doTrace = requestJson.get('doTrace', False)
    result, traceDict = cacheService.query(requestJson, doTrace = doTrace)
    result.reset_index(drop=True, inplace=True)
    dataDict = result.to_json(orient='records')
    result = jsonify({"data": json.loads(dataDict), "trace": traceDict})
    #print("Returning result", result)
    return result

@app.route('/api/reset', methods=['POST'])   
def resetCache():
    cacheService.reset()
    return jsonify({"status": "ok"})
    
    


if __name__ == '__main__':
    app.run(debug=True, port='5000')