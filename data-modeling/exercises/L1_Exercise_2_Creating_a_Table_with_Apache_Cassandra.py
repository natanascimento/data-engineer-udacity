# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 1 Exercise 2: Creating a Table with Apache Cassandra
# MAGIC <img src="images/cassandralogo.png" width="250" height="250">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Walk through the basics of Apache Cassandra. Complete the following tasks:<li> Create a table in Apache Cassandra, <li> Insert rows of data,<li> Run a simple SQL query to validate the information. <br>
# MAGIC `#####` denotes where the code needs to be completed.
# MAGIC     
# MAGIC Note: __Do not__ click the blue Preview button in the lower taskbar

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import Apache Cassandra python package

# COMMAND ----------

import cassandra

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a connection to the database

# COMMAND ----------

from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO-DO: Create a keyspace to do the work in 

# COMMAND ----------

## TO-DO: Create the keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS natan 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO-DO: Connect to the Keyspace

# COMMAND ----------

## To-Do: Add in the keyspace you created
try:
    session.set_keyspace('natan')
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Song Library that contains a list of songs, including the song name, artist name, year, album it was from, and if it was a single. 
# MAGIC 
# MAGIC `song_title
# MAGIC artist_name
# MAGIC year
# MAGIC album_name
# MAGIC single`

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO-DO: You need to create a table to be able to run the following query: 
# MAGIC `select * from songs WHERE year=1970 AND artist_name="The Beatles"`

# COMMAND ----------

## TO-DO: Complete the query below
query = "CREATE TABLE IF NOT EXISTS songs "
query = query + "(song_title text, artist_name text, year int, album_name text, single text, PRIMARY KEY (year, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ### TO-DO: Insert the following two rows in your table
# MAGIC `First Row:  "Across The Universe", "The Beatles", "1970", "False", "Let It Be"`
# MAGIC 
# MAGIC `Second Row: "The Beatles", "Think For Yourself", "False", "1965", "Rubber Soul"`

# COMMAND ----------

## Add in query and then run the insert statement
query = "INSERT INTO songs (song_title, artist_name, year, album_name, single)" 
query = query + " VALUES (%s, %s, %s, %s, %s)"

try:
    session.execute(query, ("Across The Universe", "The Beatles", 1970, "False", "Let It Be"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, ("The Beatles", "Think For Yourself", 1965, "False", "Rubber Soul"))
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO-DO: Validate your data was inserted into the table.

# COMMAND ----------

## TO-DO: Complete and then run the select statement to validate the data was inserted into the table
query = 'SELECT * FROM songs'
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.album_name, row.artist_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO-DO: Validate the Data Model with the original query.
# MAGIC 
# MAGIC `select * from songs WHERE YEAR=1970 AND artist_name="The Beatles"`

# COMMAND ----------

##TO-DO: Complete the select statement to run the query 
query = "select * from songs where year=1970 and artist_name='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.album_name, row.artist_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### And Finally close the session and cluster connection

# COMMAND ----------

session.shutdown()
cluster.shutdown()

# COMMAND ----------

