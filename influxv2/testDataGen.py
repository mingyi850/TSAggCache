import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import numpy as np
import psutil

token = "NVRAh0Hy9gLvSJVlIaYVRIP5MTktlqBHCOGxpgzIOHdSD-fu2vGjug5NmMcTv2QvH7BK6XG0tQvaoPXUWmuvLQ=="
org = "Realtime"
url = "http://localhost:8086"

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

bucket="Test"

write_api = write_client.write_api(write_options=SYNCHRONOUS)

while True:
  point = (
    Point("cpu_usage")
    .tag("platform", "mac_os")
    .tag("host", "host1")
    .field("value", psutil.cpu_percent())
  )
  point2 = (
    Point("cpu_usage")
    .tag("platform", "windows")
    .tag("host", "host1")
    .field("value", psutil.cpu_percent() + np.random.normal(0, 10))

  )
  point3 = (
    Point("cpu_usage")
    .tag("platform", "mac_os")
    .tag("host", "host2")
    .field("value", psutil.cpu_percent() - np.random.normal(0, 10))
  )
  point4 = (
    Point("cpu_usage")
    .tag("platform", "windows")
    .tag("host", "host2")
    .field("value", psutil.cpu_percent() + np.random.normal(10, 20))

  )
  write_api.write(bucket=bucket, org="Realtime", record=point)
  write_api.write(bucket=bucket, org="Realtime", record=point2)
  write_api.write(bucket=bucket, org="Realtime", record=point3)
  write_api.write(bucket=bucket, org="Realtime", record=point4)
  print("wrote points to DB")
  time.sleep(2) # separate points by 1 second
