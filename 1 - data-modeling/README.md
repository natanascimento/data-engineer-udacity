# Data Modeling 

### Difference of databases types

- Relational database:
  - SQL (Structured Query Language) is the language used across almost all relational database system for querying and maintaining the database;
  - RDBMS (Relational Database Management System);
  - A schema is a collection of tables in some database terminology;
  - Advantages:
    - Flexibility for writing in SQL queries;
    - Modeling the data not modeling queries;
    - Ability to do JOINS;
    - Ability to do aggregations and analytics;
    - Secondary indexes avaliable;
    - Smaller data volumes;
    - **ACID Transactions:**
      - Atomicity;
      - Consistency;
      - Isolation;
      - Durability;

### Data Model Types

- OLTP(On-line Transaction Processing):
  - Is involved in the operation of a particular system;
  - Large number of short on-line transactions (INSERT, UPDATE, DELETE);
  - Is put on very fast query processing;
  - Is detailed and current data, and schema used to store transacional databases in the entity model (usually 3NF);

- OLAP(On-line Analytical Processing):
  - Historical Data or Achival Data;
  - Relatively low volume of transactions;
  - Queries are often very complex and involve aggregations;
  - There is aggregated, historical data, store in multi-dimensional schemas (usually star schema or snowflake schema);

### Structuring the Database

- Normalization: 
  - To reduce data redudancy and increase data integrity;
  - The process of structuring a relational database in accordance with a series of normal forms in order to reduce data redudancy and increase data integrity;

- Denormalization: 
  - It will not feel as natural; 
  - Must be done in read heavy workloads to increase performance;

- Objectives of Normal Form:
  - To free the database from unwanted insertions, updates and deletion dependencies;
  - To reduce the need for refactoring the database as new types of data are introduced;
  - To make the relational model more informative of users;
  - to make the database neutral to the query statistics;
  - Normal Forms:
    - 1NF:
      - Atomic values: each cell contains unique and single values;
      - Be able to add data without altering tables;
      - Keep relationships between tables together with foreign keys;
    - 2NF:
      - Have reached 1NF;
      - All columns in the table must rely on the PK;
    - 3NF:
      - Must be in 2nd NF;
      - No transitive dependencies;
   
