# Duck db cli demo

**Don't forget to replace the path by your own folder path**

## 1. Install the cli
Goto the download [page](https://duckdb.org/docs/installation/index?version=stable&environment=cli&platform=linux&download_method=direct)

```shell
cd /home/pengfei/Tools/DuckDB

wget https://github.com/duckdb/duckdb/releases/download/v0.10.2/duckdb_cli-linux-amd64.zip

# unzip the bin
unzip duckdb_cli-linux-amd64.zip
```

## 2. Start a duckdb instance

```shell
# if you skip the path to db, the duckdb will run in memory mode
./duckdb

# If DuckDB is already running, use the attach command to connect to a database.
ATTACH DATABASE '/tmp/mydb.db' AS mydb;

# show available database
show databases;

# show available tables
show tables;

# get table details
select table_name, table_type from INFORMATION_SCHEMA.TABLES;

```

We can also run the duckdb directly on disk mode

```shell
# run a on disk mode duck db
./duckdb /tmp/mydb.db
```

## 3. Create tables

```shell
# create a base table from a csv file
CREATE OR REPLACE TABLE patho_csv AS SELECT * FROM read_csv_auto('/mnt/hgfs/ubuntu_share/data_set/demo_chu/pathologies.csv');

# create a view from parquet file
CREATE OR REPLACE VIEW patho_parquet AS SELECT * from read_parquet('/mnt/hgfs/ubuntu_share/data_set/demo_chu/pathologies.parquet');

```

- **The view is not physically materialized**. Instead, the query is run every time the view is referenced in a query. 
It means everytime, I use the view patho_parquet, the query `select * from read_parquet` will be called again.
- **The table is materialized (in memory/on disk)**. The select statement is executed only once.

## 4. Drop tables and views

```shell
DROP table if exists patho_csv;
DROP view if exists patho_parquet;
```

## 5. A good comprise between relational sql and no sql db.

- Create relation between tables by using foreign keys
- insert/delete rows
- ACID transaction supports

### 5.1 Create a one-to-many relations

```shell
# create two tables
CREATE TABLE students (id INTEGER PRIMARY KEY, name VARCHAR);
CREATE TABLE exams (exam_id INTEGER PRIMARY KEY, stu_id INTEGER REFERENCES students(id), grade INTEGER);
INSERT INTO students VALUES (1, 'toto');
INSERT INTO students VALUES (2, 'titi');
INSERT INTO exams VALUES (1,1, 16);
INSERT INTO exams VALUES (2,1, 18);
INSERT INTO exams VALUES (3,2, 6);
INSERT INTO exams VALUES (4,2, 8);

# show all exams of toto and titi
SELECT students.id, students.name, exams.exam_id, exams.grade
FROM exams
JOIN students ON exams.stu_id = students.id
WHERE students.name = 'toto';

# try to delete
DELETE FROM students WHERE name = 'titi';
```

### 5.2 Use transaction to safe delete

```shell
BEGIN TRANSACTION;
INSERT INTO students VALUES (3, 'tata');
COMMIT;

BEGIN TRANSACTION;
DELETE FROM students WHERE name = 'tata';
ROLLBACK;
```

## 6. Better sql experience

### 6.1 no select required

```shell
# no select required,
from patho_parquet limit 10;

# revert select
from patho_parquet select * limit 10;

```

### 6.2 exclude from select

```shell
from patho_parquet select * exclude ("patho_niv2", "patho_niv3") limit 2;
```

### 6.3 Replace column

We can use the `replace` keyword to replace old column with new columns

```shell
# use replace to convert column type
select * exclude ('patho_niv2', 'patho_niv3') replace(CAST(annee AS INT) AS annee) FROM patho_parquet;

# create a new view
create or replace view patho_clean as select * exclude ("patho_niv2", "patho_niv3") replace(CAST(annee AS INT) AS annee) FROM patho_parquet;
```

### 6.4 groupBy all

WE can use keyword `All` to replace all column names in the select statement which are not aggregation.
This can save us a lot of time, if the select is long

```shell
# classic sql
SELECT dept, sexe, annee, patho_niv1, SUM(ntop), AVG(npop)
        FROM patho_clean
        GROUP BY dept, sexe, annee, patho_niv1;

# duck db simple sql
SELECT dept, sexe, annee, patho_niv1, SUM(ntop), AVG(npop)
        FROM patho_clean
        GROUP BY ALL;
```

## 7. Various display mode

```shell
# line mode
.mode line

from sf_fire select "Call Number", "Call Type" limit 1;

# mode markdown
.mode markdown
```

The `.mode` command may be used to change the appearance of the tables returned in the terminal output. 
These include the default :
- `line` mode: print data in one line
- `duckbox` mode: default display mode
- `csv/json` mode: for ingestion by other tools, 
- `markdown/latex` mode: for generating documents
- `insert` mode for generating SQL statements.

## 8. output mode

By default, the output is printed on the stdout, we can change it to redirect output to a file

```shell
# output to a file
.output /tmp/out.md

# change it back to stdout
.output
```

## 9. Export table to csv/parquet/json

```shell
# export to csv
COPY students TO '/tmp/students.csv' (HEADER, DELIMITER ',');

# export to parquet
COPY students TO '/tmp/students.parquet' (FORMAT PARQUET);
```
