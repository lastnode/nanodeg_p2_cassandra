# Introduction

A part of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027), this [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) project looks to collect and present user activity information for a fictional music streaming service called Sparkify. The project is separated into two parts:

1) Creating an ETL Pipeline for Pre-Processing the Files

2) Inserting the data into Apache Cassandra and running queries

### Query 1 - Unoptimized and ALLOW FILTERING ON - PRIMARY KEY (session_id)

```
cqlsh:sparkify> select * from artist_song_length_by_session WHERE session_id = 338 and item_in_session = 4 ALLOW FILTERING;

 session_id | artist_name | item_in_session | song_length | song_title
------------+-------------+-----------------+-------------+---------------------------------
        338 |   Faithless |               4 |    495.3073 | Music Matters (Mark Knight Dub)

(1 rows)

Tracing session: a9954eb0-bcd2-11ea-bb8f-6b974df2e593

 activity                                                                                                                                          | timestamp                  | source    | source_elapsed | client
---------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------+-----------+----------------+-----------
                                                                                                                                Execute CQL3 query | 2020-07-03 07:42:36.251000 | 127.0.0.1 |              0 | 127.0.0.1
 Parsing select * from artist_song_length_by_session WHERE session_id = 338 and item_in_session = 4 ALLOW FILTERING; [Native-Transport-Requests-1] | 2020-07-03 07:42:36.251000 | 127.0.0.1 |            138 | 127.0.0.1
                                                                                                 Preparing statement [Native-Transport-Requests-1] | 2020-07-03 07:42:36.251000 | 127.0.0.1 |            224 | 127.0.0.1
                                                                   Executing single-partition query on artist_song_length_by_session [ReadStage-3] | 2020-07-03 07:42:36.251000 | 127.0.0.1 |            556 | 127.0.0.1
                                                                                                        Acquiring sstable references [ReadStage-3] | 2020-07-03 07:42:36.251000 | 127.0.0.1 |            591 | 127.0.0.1
                                                                                                           Merging memtable contents [ReadStage-3] | 2020-07-03 07:42:36.251000 | 127.0.0.1 |            604 | 127.0.0.1
                                                                                              Read 1 live rows and 0 tombstone cells [ReadStage-3] | 2020-07-03 07:42:36.252000 | 127.0.0.1 |            859 | 127.0.0.1
                                                                                                                                  Request complete | 2020-07-03 07:42:36.252012 | 127.0.0.1 |           1012 | 127.0.0.1
```



```





### Query 1 - Optimized - PRIMARY KEY (session_id, item_in_session)
```
cqlsh:sparkify> select * from artist_song_length_by_session WHERE session_id = 338 and item_in_session = 4;

 session_id | item_in_session | artist_name | song_length | song_title
------------+-----------------+-------------+-------------+---------------------------------
        338 |               4 |   Faithless |    495.3073 | Music Matters (Mark Knight Dub)

(1 rows)

Tracing session: d1f26820-bcd2-11ea-bb8f-6b974df2e593

 activity                                                                                                                          | timestamp                  | source    | source_elapsed | client
-----------------------------------------------------------------------------------------------------------------------------------+----------------------------+-----------+----------------+-----------
                                                                                                                Execute CQL3 query | 2020-07-03 07:43:43.970000 | 127.0.0.1 |              0 | 127.0.0.1
 Parsing select * from artist_song_length_by_session WHERE session_id = 338 and item_in_session = 4; [Native-Transport-Requests-1] | 2020-07-03 07:43:43.970000 | 127.0.0.1 |             77 | 127.0.0.1
                                                                                 Preparing statement [Native-Transport-Requests-1] | 2020-07-03 07:43:43.970000 | 127.0.0.1 |            123 | 127.0.0.1
                                                   Executing single-partition query on artist_song_length_by_session [ReadStage-3] | 2020-07-03 07:43:43.970001 | 127.0.0.1 |            260 | 127.0.0.1
                                                                                        Acquiring sstable references [ReadStage-3] | 2020-07-03 07:43:43.970001 | 127.0.0.1 |            275 | 127.0.0.1
                                                                                           Merging memtable contents [ReadStage-3] | 2020-07-03 07:43:43.970001 | 127.0.0.1 |            283 | 127.0.0.1
                                                                              Read 1 live rows and 0 tombstone cells [ReadStage-3] | 2020-07-03 07:43:43.970001 | 127.0.0.1 |            394 | 127.0.0.1
                                                                                                                  Request complete | 2020-07-03 07:43:43.970494 | 127.0.0.1 |            494 | 127.0.0.1
```