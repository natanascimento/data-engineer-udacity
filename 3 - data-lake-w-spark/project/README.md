# Data Lake Project

## Summary 

For a more accurate analysis of the data, a fictional music streaming startup called Sparkify. With the strong growth of the music streaming business, Sparkify has seen an increase in its database and with that it wants to move from the data warehouse to the data lake.

The lake is located on Amazon S3, and has log directories in JSON format, these logs represent user activity in the application, in addition there is metadata in JSON format referring to the songs in the application.

Based on this use case, a pipeline of data processing from S3 was thought in the bronze layer (raw data) to the silver layer (refined data), the processing is done using Apache Spark building models of dimensional and fact tables .

## Pipeline

- Create a Spark Session with Apache Hadoop into the Amazon Web Service Support module;
- Load AWS Access Key and AWS Session Key into the dl.cfg;
- Ingest log and song data files from the S3 raw data (bronze layer);
- Clean and process data to produce for the silver layer (silver layer);
    - Add unique row indetifiers to all fact and dim tables;
    - Remove duplicate rows when the data is readed;
    - Setting up null to desired values;
    - Parse timestamps into the time and date components;
    - Create tables;
    - Write data to the silver layer;
    
## Tables 

Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
users - users in the app
    - user_id, first_name, last_name, gender, level
songs - songs in music database
    - song_id, title, artist_id, year, duration
artists - artists in music database
    - artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday
    
    
## How to Run:

To stage the pipeline you need run the ```python3 etl.py```.

After the conclusion you will see the pipeline execution time.