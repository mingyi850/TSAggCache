import time
import unittest
from queryDSL import InfluxQueryBuilder, QueryFilter, QueryAggregation, OrQueryFilter, AndQueryFilter

class TestInfluxQueryBuilder(unittest.TestCase):
    def test_withGroupKeys(self):
        builder = InfluxQueryBuilder()
        builder.withGroupKeys(["key1"])
        self.assertEqual(builder.groupKeys, ["key1"])
        builder.withGroupKeys(["key2"])
        self.assertEqual(builder.groupKeys, ["key1", "key2"])
    
    def test_withMeasurements(self):
        builder = InfluxQueryBuilder()
        builder.withMeasurements(["measurement1"])
        self.assertEqual(builder.measurements, ["measurement1"])
        builder.withMeasurements(["measurement2"])
        self.assertEqual(builder.measurements, ["measurement1", "measurement2"])
    
    def test_withBucket(self):
        builder = InfluxQueryBuilder()
        builder.withBucket("my_bucket")
        self.assertEqual(builder.bucket, "my_bucket")
    
    def test_withFilter(self):
        builder = InfluxQueryBuilder()
        filter1 = QueryFilter("key1", "value1")
        builder.withFilter(filter1)
        self.assertEqual(builder.filters, [filter1])
        filter2 = QueryFilter("key2", "value2")
        builder.withFilter(filter2)
        self.assertEqual(builder.filters, [filter1, filter2])
    
    def test_withTable(self):
        builder = InfluxQueryBuilder()
        builder.withTable("my_table")
        self.assertEqual(builder.table, "my_table")
    
    def test_withAggregate(self):
        builder = InfluxQueryBuilder()
        aggregate = QueryAggregation("mean", "1h", False)
        builder.withAggregate(aggregate)
        self.assertEqual(builder.aggregate, aggregate)
    
    def test_withRange(self):
        builder = InfluxQueryBuilder()
        builder.withRange("1609459200", "1609545600")
        self.assertEqual(builder.range.start, 1609459200)
        self.assertEqual(builder.range.end, 1609545600)
    
    def test_withRelativeRange(self):
        builder = InfluxQueryBuilder()
        builder.withRelativeRange("1d", "2d")
        self.assertEqual(builder.relativeRange.fr, "1d")
        self.assertEqual(builder.relativeRange.to, "2d")
    
    def test_getRangeFromRelative(self):
        builder = InfluxQueryBuilder()
        builder.withRelativeRange("1d", "2d")
        range = builder.getRangeFromRelative()
        now = time.time()
        self.assertAlmostEqual(range.start, int(now - 86400), 0)
        self.assertAlmostEqual(range.end, int(now - 172800), 0)
    
    def test_withYield(self):
        builder = InfluxQueryBuilder()
        builder.withYield("my_yield")
        self.assertEqual(builder.yield_name, "my_yield")
    
    def test_buildFluxStr(self):

        expectedFluxStr = """from(bucket: "my_bucket")
        |> range(start: 1609459200, stop: 1609545600)
        |> filter(fn: (r) => r["key1"] == "value1")
        |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
        |> yield(name: "my_yield")""".strip().replace("\n", "").replace(" ", "")
        builder = InfluxQueryBuilder()
        builder.withBucket("my_bucket")
        builder.withRange("1609459200", "1609545600")
        filter1 = QueryFilter("key1", "value1")
        builder.withFilter(filter1)
        aggregate = QueryAggregation("1h", "mean")
        builder.withAggregate(aggregate)
        builder.withYield("my_yield")
        flux_str = builder.buildFluxStr()
        self.assertEqual(flux_str.strip().replace("\n", "").replace(" ", ""), expectedFluxStr)
    
    def test_getAggregateMeasurements(self):
        builder = InfluxQueryBuilder()
        builder.withMeasurements(["measurement1"])
        builder.withMeasurements(["measurement2"])
        aggregate = QueryAggregation("1h", "mean", False)
        builder.withAggregate(aggregate)
        aggregate_measurements = builder.getAggregateMeasurements()

        expected_aggregate_measurements = "mean(measurement1) as measurement1, mean(measurement2) as measurement2"
        self.assertEqual(aggregate_measurements, expected_aggregate_measurements)
    
    def test_parseFilter(self):
        builder = InfluxQueryBuilder()
        filter1 = QueryFilter("key1", "value1")
        filter2 = QueryFilter("key2", "value2")
        or_filter = OrQueryFilter([filter1, filter2])
        and_filter = AndQueryFilter([filter1, filter2])
        parsed_filter1 = builder.parseFilter(filter1)
        parsed_filter2 = builder.parseFilter(or_filter)
        parsed_filter3 = builder.parseFilter(and_filter)

        expected_parsed_filter1 = "key1 = 'value1'"
        expected_parsed_filter2 = "key1 = 'value1' OR key2 = 'value2'"
        expected_parsed_filter3 = "key1 = 'value1' AND key2 = 'value2'"

        self.assertEqual(parsed_filter1, expected_parsed_filter1)
        self.assertEqual(parsed_filter2, expected_parsed_filter2)
        self.assertEqual(parsed_filter3, expected_parsed_filter3)
    
    def test_parseFilters(self):
        builder = InfluxQueryBuilder()
        filter1 = QueryFilter("key1", "value1")
        filter2 = QueryFilter("key2", "value2")
        filters = [filter1, filter2]
        parsed_filters = builder.parseFilters(filters)
        expected_parsed_filters = "key1 = 'value1' AND key2 = 'value2'"
        self.assertEqual(parsed_filters, expected_parsed_filters)
    
    def test_getGroupByInflux(self):
        builder = InfluxQueryBuilder()
        aggregate = QueryAggregation("1h", "mean", False)
        builder.withAggregate(aggregate)
        builder.withGroupKeys(["key1"])
        group_by_influx = builder.getGroupByInflux()
        expected_group_by_influx = "GROUP BY time(1h), key1\n"
        self.assertEqual(group_by_influx, expected_group_by_influx)
    
    def test_buildInfluxQlStr(self):
        builder = InfluxQueryBuilder()
        builder.withRelativeRange("1h", None)
        filter1 = QueryFilter("key1", "value1")
        builder.withFilter(filter1)
        builder.withTable("my_table")
        builder.withGroupKeys(["key1"])
        builder.withMeasurements(["measurement1"])
        builder.withAggregate(QueryAggregation("1m", "mean"))
        influxql_str = builder.buildInfluxQlStr()
        expected_influxql_string = """SELECT mean(measurement1) as measurement1
            FROM my_table 
            WHERE key1 = 'value1'
            AND time > now() - 1h
            GROUP BY time(1m), key1""".strip().replace("\n", "").replace(" ", "")

        self.assertEqual(influxql_str.strip().replace("\n", "").replace(" ", ""), expected_influxql_string)
    
    def test_buildJson(self):
        builder = InfluxQueryBuilder()
        builder.withBucket("my_bucket")
        builder.withRange("1609459200", "1609545600")
        filter1 = QueryFilter("key1", "value1")
        builder.withFilter(filter1)
        aggregate = QueryAggregation("1h", "mean")
        builder.withAggregate(aggregate)
        builder.withYield("my_yield")
        builder.withGroupKeys(["key1"])
        builder.withMeasurements(["measurement1", "measurement2"])
        withTable = builder.withTable("my_table")
        expected_json = {
            "bucket": "my_bucket",
            "range": {
                "start": 1609459200,
                "end": 1609545600
            },
            "filters": [
                {
                    "key": "key1",
                    "value": "value1",
                    "type": "raw"
                }
            ],
            "aggregate": {
                "aggFunc": "mean",
                "timeWindow": "1h",
                "createEmpty": False
            },
            "relativeRange": None,
            "yield": "my_yield",
            "measurements": ["measurement1", "measurement2"],
            "table": "my_table",
            "groupKeys": ["key1"]
        }
        json = builder.buildJson()
        self.assertEqual(json, expected_json)
    
    def test_fromJson(self):
        json = {
            "bucket": "my_bucket",
            "range": {
                "start": 1609459200,
                "end": 1609545600
            },
            "filters": [
                {
                    "key": "key1",
                    "value": "value1"
                }
            ],
            "aggregate": {
                "aggFunc": "mean",
                "timeWindow": "1h"
            },
            "yield": "my_yield",
            "measurements": ["measurement1", "measurement2"],
            "table": "my_table",
            "groupKeys": ["key1"]
        }
        builder = InfluxQueryBuilder.fromJson(json)
        self.assertEqual(builder.bucket, "my_bucket")
        self.assertEqual(builder.range.start, 1609459200)
        self.assertEqual(builder.range.end, 1609545600)
        self.assertEqual(len(builder.filters), 1)
        self.assertEqual(builder.filters[0].key, "key1")
        self.assertEqual(builder.filters[0].value, "value1")
        self.assertEqual(builder.aggregate.aggFunc, "mean")
        self.assertEqual(builder.aggregate.timeWindow, "1h")
        self.assertEqual(builder.yield_name, "my_yield")
        self.assertEqual(builder.measurements, ["measurement1", "measurement2"])
        self.assertEqual(builder.table, "my_table")
        self.assertEqual(builder.groupKeys, ["key1"])

if __name__ == '__main__':
    unittest.main()