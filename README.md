# Udacity Project: Spark and Data Lakes
In this project, we'll use PySpark to create an end-to-end data pipeline, to 
1. parse JSON logs from files store in S3
2. transform the datasets to create fact and dimension tables
3. store the new datasets to partitioned parquet files in S3

## Overview
The goal of this project is to build an ETL for songs and user activity on a fictional music streaming app, for analytics purposes.  

## Available Files
The project contains several files and folders, listed bellow

├── README.md  
├── dl.cnf  
├── etl.py  
└── test_notebook.ipynb  

Document details can be found bellow,
- `dl.cnf` config file to store AWS credentials  
- `etl.py` file to read and process files for `song_data` and `log_data` in S3 and load them into S3. 

Testing scripts (no need to run), 
- `test_notebook.ipynb` jupyter notebook that displays the first few rows of each table to let one check the database (sanity check).

## S3 Setup
The project requires the following folders to be present in S3, in `s3://udacity-dend/`
- `log_data` folder that holds simulation based user event logs in JSON format.
- `song_data` folder that holds a subset of the [Million Song Dataset](http://millionsongdataset.com/) in JSON format.

Next, the `s3://udacity-davg/sparkifydb/` path needs to be created in s3, with appropriate access for the EMR cluster.

## Execution
One can use the command line to parse data from S3, transform, and store data in S3, by executing
```bash
$ python3 etl.py 
```

## Database Overview
### Fact Table(s)
```
songplays
- description: records in log data associated with song plays (page:NextSong)
- columns:
    - start_time: TIMESTAMP
    - user_id: STRING
    - level: STRING
    - song_id: STRING
    - artist_id: STRING
    - session_id: INTEGER
    - location: STRING
    - user_agent: STRING
    - year: INTEGER
    - month: INTEGER
- location: s3://udacity-davg/sparkifydb/songplays.parquet/
- partition: year, month
```

### Dimension Table(s)
```
users 
- description: users in the app
- columns:
    - user_id: STRING
    - first_name: STRING
    - last_name: STRING
    - gender: STRING
    - level: STRING
- location: s3://udacity-davg/sparkifydb/users.parquet/
```
```
songs 
- description: songs in music database
- columns:
    - song_id: STRING
    - title: STRING
    - artist_name: STRING
    - duration: DOUBLE
    - year: INTEGER
    - artist_id: STRING
- location: s3://udacity-davg/sparkifydb/songs.parquet/
- partition: year, artist_id
```
```
artists 
- description: artists in music database
- columns:
    - artist_id: STRING
    - name: STRING
    - location: STRING
    - latitude: DOUBLE
    - longitude: DOUBLE
- location: s3://udacity-davg/sparkifydb/artists.parquet/
```
```
time 
- description: timestamps of records in songplays broken down into specific units
- columns:
    - start_time: TIMESTAMP
    - hour: INTEGER
    - day: INTEGER
    - week: INTEGER
    - month: INTEGER
    - year: INTEGER
    - weekday: INTEGER
- location: s3://udacity-davg/sparkifydb/time.parquet/
- partition: year, month
```
