# Introduction

A part of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027), this [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) project looks to collect and present user activity information for a fictional music streaming service called Sparkify. The project is separated into two parts:

### 1) Creating an ETL Pipeline for Pre-Processing the Files

In this part of the project, we process the raw data found in the `.csv` files in the `event-data/` folder. Extracting the data we want from them, we combine them into a single `event_datafile_new.csv` file. 

### 2) Inserting the data into Apache Cassandra and running queries

We then read the data in the `event_datafile_new.csv` file and load them into Cassandra tables that we create in order to run specific queries. The rationale for each table's unique design is discussed in-line.

Each section can be run cell-by-cell, and the output for each of the queries will appear immediately below where they are run.

# Files
```
- event_data/ -- the folder with user activity information, in .csv format
- images/-- folder for example screenshots
- event_datafile_new.csv -- the file we output via the ETL pipeline (see #1 above)
- Project_1B.ipynb -- the Jupyter notebook that contains all the Python code
- README.md -- this file
```

# Setup

In order to run these Python scripts, you will first need to install Python 3 on your computer, and then install the following Python modules via [pip](https://pypi.org/project/pip/) or [anaconda](https://www.anaconda.com/products/individual):

- [jupyter](https://jupyter.org/) - a package that allows you to open Jupyter notebooks.
- [cassandra-driver](https://docs.datastax.com/en/developer/python-driver/3.23/) - a Cassandra database driver for Python.
- [Numpy](https://numpy.org/) - a math/science package for Python.
- [Pandas](https://pandas.pydata.org/) - a data analysis package for Python.

To install these via `pip` you can run:

`pip install jupyterlab cassandra-driver numpy pandas`

After these packages have been successfully installed, head to the project folder and open a Jupyter notebook by running:

`jupyter notebook`

From the web-based Jupyter interface that pops up, click the `Project_1B.ipynb` file to get started. This [introduction to Jupyter Notebooks](https://realpython.com/jupyter-notebook-introduction/) may help as well!

# Table and Query Design

In order to optimize for read speeds, we have chosen to use different Primary Key designs for the three tables. The rationale for each design decision has been documented in the Jupyter Notebook itself (`Project_1B.ipynb`), but we've also used Cassandra's [TRACING](https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlshTracing.html) command to measure actual performance for one query. As seen in the results below, `QUERY 1B` which was optimized using a Composite Primary Key was more than twice as fast as `QUERY 1A` which was unoptimized and therefore had to use Cassandra's `ALLOW FILTERING` option.

### Query 1A - Unoptimized and ALLOW FILTERING ON - PRIMARY KEY (session_id)

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


### Query 1B - Optimized - PRIMARY KEY (session_id, item_in_session)
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