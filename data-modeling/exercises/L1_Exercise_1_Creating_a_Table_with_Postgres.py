# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 1 Exercise 1: Creating a Table with PostgreSQL
# MAGIC 
# MAGIC <img src="images/postgresSQLlogo.png" width="250" height="250">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Walk through the basics of PostgreSQL. You will need to complete the following tasks:<li> Create a table in PostgreSQL, <li> Insert rows of data <li> Run a simple SQL query to validate the information. <br>
# MAGIC `#####` denotes where the code needs to be completed. 
# MAGIC     
# MAGIC Note: __Do not__ click the blue Preview button in the lower task bar

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import the library 
# MAGIC *Note:* An error might popup after this command has executed. If it does, read it carefully before ignoring. 

# COMMAND ----------

import psycopg2

# COMMAND ----------

!echo "alter user student createdb;" | sudo -u postgres psql

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a connection to the database

# COMMAND ----------

try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use the connection to get a cursor that can be used to execute queries.

# COMMAND ----------

try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO-DO: Set automatic commit to be true so that each action is committed without having to call conn.commit() after each command. 

# COMMAND ----------

# TO-DO: set automatic commit to be true
try:
    conn.rollback()
    conn.autocommit=True
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO-DO: Create a database to do the work in. 

# COMMAND ----------

## TO-DO: Add the database name within the CREATE DATABASE statement. You can choose your own db name.
try: 
    cur.execute("create database natan")
except psycopg2.Error as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### TO-DO: Add the database name in the connect statement. Let's close our connection to the default database, reconnect to the Udacity database, and get a new cursor.

# COMMAND ----------

## TO-DO: Add the database name within the connect statement
try: 
    conn.close()
except psycopg2.Error as e:
    print(e)
    
try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=natan user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)
    
try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)

conn.set_session(autocommit=True)

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

## TO-DO: Finish writing the CREATE TABLE statement with the correct arguments
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS songs (song_title varchar, artist_name varchar, year varchar, album_name varchar, single varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO-DO: Insert the following two rows in the table
# MAGIC `First Row:  "Across The Universe", "The Beatles", "1970", "False", "Let It Be"`
# MAGIC 
# MAGIC `Second Row: "The Beatles", "Think For Yourself", "False", "1965", "Rubber Soul"`

# COMMAND ----------

## TO-DO: Finish the INSERT INTO statement with the correct arguments

try: 
    cur.execute("INSERT INTO songs (song_title, artist_name, year, album_name, single) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 ("Across The Universe", "The Beatles", "1970", "False", "Let It Be"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO songs (song_title, artist_name, year, album_name, single) \
                  VALUES (%s, %s, %s, %s, %s)",
                  ("The Beatles", "Think For Yourself", "False", "1965", "Rubber Soul"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO-DO: Validate your data was inserted into the table. 

# COMMAND ----------

## TO-DO: Finish the SELECT * Statement 
try: 
    cur.execute("SELECT * FROM songs;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

# COMMAND ----------

# MAGIC %md
# MAGIC ### And finally close your cursor and connection. 

# COMMAND ----------

cur.close()
conn.close()