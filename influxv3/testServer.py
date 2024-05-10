import json
from flask import Flask, jsonify, request
from queryDSL import InfluxQueryBuilder, QueryAggregation, QueryFilter
from CacheService import CacheService
import Utilsv3
import configparser

config = configparser.ConfigParser()
config.read('cacheConfig.ini')
cacheConfig = config['cache']
port = cacheConfig.get('port', 5000)
name = cacheConfig.get('name', 'cacheService')
app = Flask(name)
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
def serializeResult(result):
    result.reset_index(drop=True, inplace=True)
    return result.to_json(orient='records')

@app.route('/api/query', methods=['POST'])
def query():
    #print(request.data.decode('utf-8'))
    #print(request.data)
    requestJson = json.loads(request.data)
    doTrace = requestJson.get('doTrace', False)
    result, traceDict = cacheService.query(requestJson, doTrace = doTrace)
    
    dataJson = Utilsv3.withTrace(doTrace, traceDict, "SerializeResult", lambda: serializeResult(result))
    result = jsonify({"data": json.loads(dataJson), "trace": traceDict})
    #print("Returning result", result.data)
    return result

@app.route('/api/reset', methods=['POST'])   
def resetCache():
    cacheService.reset()
    return jsonify({"status": "ok"})
    
    


if __name__ == '__main__':
    app.run(debug=True, port=port)