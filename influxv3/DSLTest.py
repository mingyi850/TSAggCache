from queryDSL import InfluxQueryBuilder, QueryAggregation, QueryFilter

influxBuilder = (InfluxQueryBuilder()
               .withBucket("Test")
               .withMeasurements(["cpu_usage", "temperature"])
               .withTable("system_metrics")
               .withFilter(QueryFilter("platform", "mac_os").OR(QueryFilter("platform", "windows")))
               .withAggregate(QueryAggregation("1m", "mean", False))
               .withRelativeRange('300m', None)
               .withGroupKeys(["host", "platform"])
       )

query2 = f"""
SELECT 
mean(cpu_usage) as mean_cpu_usage, 
mean(temperature) as mean_temperature
FROM system_metrics
WHERE platform = 'mac_os' OR platform = 'windows'
AND time > now() - 300m
GROUP BY time(1m), host,platform
"""

print(influxBuilder.buildJson())
print(influxBuilder.buildFluxStr())