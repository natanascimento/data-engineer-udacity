# Sparkify Postgres ETL

This is the first project submission for the Data Engineering Nanodegree.
This project consists on putting into practice the following concepts:
- Data modeling with Postgres
- Database star schema created 
- ETL pipeline using Python

## Context

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

### Data
- **Song datasets**: all json files are nested in subdirectories under */data/song_data*. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: all json files are nested in subdirectories under */data/log_data*. A sample of a single row of each files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```

## Database Schema
The schema used for this exercise is the Star Schema: 

There is one main fact table containing all the measures associated to each event (user song plays), 
and 4 dimentional tables, each with a primary key that is being referenced from the fact table.

#### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page NextSong
- songplay_id (INT)
- start_time (DATE)
- user_id (INT)
- level (TEXT)
- song_id (TEXT)
- artist_id (TEXT)
- session_id (INT)
- location (TEXT)
- user_agent (TEXT)

#### Dimension Tables
**users** - users in the app
- user_id (INT)
- first_name (TEXT)
- last_name (TEXT)
- gender (TEXT)
- level (TEXT)

**songs** - songs in music database
- song_id (TEXT)
- title (TEXT)
- artist_id (TEXT)
- year (INT)
- duration (FLOAT)

**artists** - artists in music database
- artist_id (TEXT)
- name (TEXT)
- location (TEXT)
- lattitude (FLOAT)
- longitude (FLOAT)

**time** - timestamps of records in songplays broken down into specific units
- start_time (DATE)
- hour (INT)
- day (INT)
- week (INT)
- month (INT)
- year (INT)
- weekday (TEXT)

Database Design is very optimized because with a ew number of tables and doing specific join, we can get the most information and do analysis

## Pipeline

To process the data in general, a method of centralizing the stages was used, so we can guarantee that all the stages of the process will be carried out. As soon as each process is completed, a message is sent informing how the progress is, at the end of everything it is validated if it was actually executed and to return a "completed" message, informing that a new pipeline can be executed.

1° - Create all tables;
2° - Extract, transform and Load data to local PostgreSQL

To run this please insert into the terminal

```python
python data_pipeline.py
```

This command will run the entire process described above