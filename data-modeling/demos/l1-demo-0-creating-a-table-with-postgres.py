# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 1 Demo 0: PostgreSQL and AutoCommits
# MAGIC 
# MAGIC <img src="images/postgresSQLlogo.png" width="250" height="250">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Walk through the basics of PostgreSQL autocommits 

# COMMAND ----------

## import postgreSQL adapter for the Python
import psycopg2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a connection to the database
# MAGIC 1. Connect to the local instance of PostgreSQL (*127.0.0.1*)
# MAGIC 2. Use the database/schema from the instance. 
# MAGIC 3. The connection reaches out to the database (*studentdb*) and use the correct privilages to connect to the database (*user and password = student*).

# COMMAND ----------

conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use the connection to get a cursor that will be used to execute queries.

# COMMAND ----------

cur = conn.cursor()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a database to work in

# COMMAND ----------

cur.execute("select * from test")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Error occurs, but it was to be expected because table has not been created as yet. To fix the error, create the table. 

# COMMAND ----------

cur.execute("CREATE TABLE test (col1 int, col2 int, col3 int);")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Error indicates we cannot execute this query. Since we have not committed the transaction and had an error in the transaction block, we are blocked until we restart the connection.

# COMMAND ----------

conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
cur = conn.cursor()

# COMMAND ----------

# MAGIC %md
# MAGIC In our exercises instead of worrying about commiting each transaction or getting a strange error when we hit something unexpected, let's set autocommit to true. **This says after each call during the session commit that one action and do not hold open the transaction for any other actions. One action = one transaction.**

# COMMAND ----------

# MAGIC %md
# MAGIC In this demo we will use automatic commit so each action is commited without having to call `conn.commit()` after each command. **The ability to rollback and commit transactions are a feature of Relational Databases.**

# COMMAND ----------

conn.set_session(autocommit=True)

# COMMAND ----------

cur.execute("select * from test")

# COMMAND ----------

cur.execute("CREATE TABLE test (col1 int, col2 int, col3 int);")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Once autocommit is set to true, we execute this code successfully. There were no issues with transaction blocks and we did not need to restart our connection. 

# COMMAND ----------

cur.execute("select * from test")

# COMMAND ----------

cur.execute("select count(*) from test")
print(cur.fetchall())

# COMMAND ----------

