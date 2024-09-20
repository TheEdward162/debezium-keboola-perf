ENGINE PERF TEST
===
This is a simple test to measure the performance of the engine. 


How to Run
---
create `conf/database.properties` file with the standard set of debezium connection properties. Then run 
`io.debezium.perf.keboola.Main` class.

Results
---
On a local M2 macbook, 3 000 000 records, single thread. Only comparable among each other, and needs testing on real environments.

| Description | Runtime | Performance [r/s] | Of Baseline |
|-------------|---------|------------------:|:------------|
| Baseline | 00:00:14.768 | 203141.0 | 1 |
| TestConsumer with markProcessed | 00:00:14.989 | 200146.8 | 0.9852604840972526 |
| TestConsumer with .toString() to disk | 00:00:33.446 | 89696.8 | 0.44154946564209097 |
| ^ + OutputStreamWriter | 00:00:18.453 | 162575.2 | 0.8003071758039982 |
| ^ + BufferedStreamWriter | 00:00:17.795 | 168586.7 | 0.8298999217292423 |
| ^ + ramfs | 00:00:16.232 | 184820.1 | 0.9098119040469428 |
| write something for each record to duckdb appender | 00:00:16.579 | 180951.8 | 0.8907694655436371 |
| ^ + write real data | 00:00:25.020 | 119904.1 | 0.5902506141054735 |
| ^ + with event order column | 00:00:25.222 | 118943.8 | 0.5855233556987511 |
| write same data but to a csv | 00:00:16.766 | 178933.6 | 0.8808344942675285 |
