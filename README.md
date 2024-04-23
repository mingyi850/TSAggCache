# Agg Cache

A cache for time series data meant to support repeated queries

## Problem

Time series data is huge, with Twitter's Observability stack collecting 170 million metrics every minute and serving 200 million queries per day. Time series databases are typically used for metrics and log storage for large systems and applications, and are typically append-only (due to the historical nature of the data)
One common use of Timeseries data is dashboarding, where users are able to create visualizations to track specific metrics over time.

However, many clients achieve this by simply re-querying data repeatedly or on startup. This repeated queries can put a strain on the time series database by repeatedly querying and aggregating the same data over and over again, which is not required in most cases.

## Proposal 
We propose a cache layer in-between the Time Series database which reduces the amount of data the database needs to handle.
The cache layer should provide the following
1. Query Aware caching - given an existing query, we can retrieve data from the cache and perform required aggregations.
2. Aggregated queries
    - Due to the density of timeseries data, most queries downsample queries by aggregating data over fixed time ranges i.e granularities 
    - Aggregations are then commonly performed over the range of data captured in these granularities
    - We want to cache these aggregations to avoid performing repeated aggregations - something that is not done in existing time series caches (link to TS Cache)
3. Aggregations - Due to the associative nature of most aggregations, they can be performed with a subset of the data
    - Max
    - Min
    - Mean
    - Count
    - Sum
3. We examine some simple cases below
    1. Get all temperatures over last 1 hours ->
        1. This is a basic query. A TS DB would find the required range and return all datapoints here.
        2. On a repeat query, We look for cache the existing data in cache and return the same result.
            - However, since the time of the second query is different from that of the first, there is a subset of data which we have yet to retrieve.
            let k be the lookback
            Example: 
               ``` - (q, t1) -> retrieve all data with timestamp t1 - k |--------------|t1.....
                - (q, t2) -> retrieve all data with timestamp t2 - k xxxxx|----------------|t2```
            In this scenario, we see that we only need to retrieve points from t2-t1, and remove points t2-k - t1-k from our result.
            Thus, the query to the timeseries database can be reduced to (get all temperatures from t1 - t2). This should reduce load on the database as well as bandwidth usage
    2. Select all temperatures over the last 1 hours, with the granularity of 1 min
            1. Using the same principle, we can see that the data involved is the same.
                t1 - k |--------------|t1(..)(..).
                t2 - k xxxxx|----------------|t2
            2. We can simply retrieve points from t2-t1 and perform aggregation on those datapoints. Any ungrouped (yet to be formed) datapoints can be stored in a combined format ((start, end), avg, count) such that future queries can re-use this aggregation. Instead of performing 60 aggregation operations, we reduce the amount of aggregations to (t2- t1) / min
        3. The last case is slighly more complex, involving multiple timeseries, but can easily be handled by caching the results.
            1. Get percentage of cpu usage used by service a (CpuUsage(a) / TotalCpuUsage), with granularity of 1 minute.
                - in order to do this, we need to retrieve 2 time series, aggregate them and perform a divide operation.
                - We need to maintain 2 separate caches 
                1. The first is for CPUUsage(a) -> We granularize this by 1 min using the average function
                2. The second is the same thing for TotalCPUUsage.
                3. We store in cache the existing CPUUsage(a) / TotalCpuUsage for each 1 min period.
                4. During a re-query, the query is submitted but only for the un-covered time period.


## Measure/Metrics
- We will compare the performance of raw queries over our caching system to measure performance gains in terms of 
 - Total Query latency
 - Bandwidth utilization (how much data is served from the DB to the cache)

## Tangible Steps for execution
- Setup connection to Influx DB/Timescale DB (preferably Influx due to it's more traditional TSDB structure)
- Create a set of queries for measurement 
- Generate dummy test data (either from a real application or using a generator)
- Create cache (using C/Rust/Golang/Scala?) to serve queries
    - Alternatively, we can create client which does caching (just as a prototype, since it is simpler to write, don't have to create elaborate key-data stores). Downside is we don't see how the additional latency of a 2-step request affects performance (but we can ballpark it)
    - Cache/Client can hold data in memory, modify query based on existing data and query DB for additional datapoints required.
- Run comparison using 2 clients
    - Standard Client directly connecting to Influx
    - Modified Client connecting to Influx via cache service

We might not use HPC, but we could probably use GCP to gain enough resources to run our experiments.

partialed = "measurement"
First find all series with matching measurements
Search inverse index for AND of measurements
if exists AND of measurments, continue
    else: Find partial match (OR of measurements)
    set partialled to measurement

For each, check that aggregation is aggregatable
Check time range contains searched time range, else set "partial"

Scan full/partial matches for filters
if filters are exactly the same, we can continue
If filters are looser (subset of) current, we drop
If filters are stricter than current, we set partialled to "filters" (if partialled already set we return False)

Lookup group matching
group matching. 
If searched groupkeys exactly match, good
if searched groupkeys are subset of cached groupkeys, good
if searched groupkeys are superset of group keys, we drop.

Group:
if existing is grouped by (A, B, C) and we search for groupBy (A, B) -> we can reconstruct 
if existing is grouped by (A, B) and we search for groupBy (A, B, C) -> Theres nothing we can do since we don't have information about C

filter matching
existing:
A AND (B OR C OR D)
new: (A AND B) OR (A AND C) OR (A AND D)

search:
A AND (C OR D OR E)
search: (A AND C) OR (A AND D) or (A AND E)
we can simply take the overlapping regions:
    (A AND C), (A AND D) and query for (A AND E)

Assume we don't have knowledge on the filter contents for each datapoint (it is not in grouping)

Then we must reject the query.

If existing is (A AND B)
and our search is A AND (B OR C)
then our search is for a superset of the existing data. We can use them.

Last exercies:
Existing is (A AND B) OR C -> DNF  already
Existing is (A AND B AND (C OR D)) -> (A AND B AND C) OR ( A AND B AND D)


End of the day - we want to simplify the cache structure.
We store entire series
- Filters (non-negotiable) - match by key
- Measurements: always fetch * (we can filter later)
- Aggregation
    - aggFn: non-negotiable
    - aggWindow: window
- Grouping (Set of fields to group on)

If data does not exist in range, we will pull new data for the entire table.

We index query based on key
- Table
- aggFn
- Filters - convert to sets for similarity checks
- grouping (check that search is subset)