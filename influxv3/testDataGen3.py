import influxdb_client, os, time
import os, time
from influxdb_client_3 import InfluxDBClient3, Point
import numpy as np
import psutil

token = "VJK1PL0-qDkTIpSgrtZ0vq4AG02OjpmOSoOa-yC0oB1x3PvZCk78In9zOAGZ0FXBNVkwoJ_yQD6YSZLx23WElA=="
org = "Realtime Big Data"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

client = InfluxDBClient3(host=host, token=token, org=org)

bucket="Test"

database="Test"

data = {
  "point1": {
    "location": "Klamath",
    "species": "bees",
    "count": 23,
  },
  "point2": {
    "location": "Portland",
    "species": "ants",
    "count": 30,
  },
  "point3": {
    "location": "Klamath",
    "species": "bees",
    "count": 28,
  },
  "point4": {
    "location": "Portland",
    "species": "ants",
    "count": 32,
  },
  "point5": {
    "location": "Klamath",
    "species": "bees",
    "count": 29,
  },
  "point6": {
    "location": "Portland",
    "species": "ants",
    "count": 40,
  },
}

while True:
  point = (
    Point("system_metrics")
    .tag("platform", "mac_os")
    .tag("host", "host1")
    .field("cpu_usage", psutil.cpu_percent())
    .field("temperature", np.random.normal(20, 2))
    .field("memory_usage", psutil.virtual_memory().percent)
  )

  point2 = (
    Point("system_metrics")
    .tag("platform", "windows")
    .tag("host", "host1")
    .field("value", psutil.cpu_percent() + np.random.normal(0, 10))
    .field("temperature", np.random.normal(23, 3))
    .field("memory_usage", psutil.virtual_memory().percent + np.random.normal(0, 8))
  )

  point3 = (
    Point("system_metrics")
    .tag("platform", "mac_os")
    .tag("host", "host2")
    .field("value", psutil.cpu_percent() - np.random.normal(0, 10))
    .field("temperature", 5 + np.random.normal(20, 1))
    .field("memory_usage", psutil.virtual_memory().percent - np.random.normal(0, 5))
  )

  point4 = (
    Point("system_metrics")
    .tag("platform", "windows")
    .tag("host", "host2")
    .field("value", psutil.cpu_percent() + np.random.normal(10, 20))
    .field("temperature", 5 + np.random.normal(20, 3))
    .field("memory_usage", psutil.virtual_memory().percent + np.random.normal(4, 16))
  )
  client.write(database=database, record=point)
  client.write(database=database, record=point2)
  client.write(database=database, record=point3)
  client.write(database=database, record=point4)
  print("wrote points to DB")
  time.sleep(2) # separate points by 2 seconds

