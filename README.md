# Take-Home-Assignment-Perfect-World-Entertainment

## Introduction
Three test files in JSON lines format have been provided and contain meta data of users event activity log. In this submission, data has been process with PySpark and turned into CSV format that was loaded into a MySQL database with event tables. A report is generated for loaded data with sql queries.


## Table design
Event tables are designed to optimize queries for report. Time-series data with relevant inforamtion for each event type has been logged to a different event type table.

## Exploratory
- etl-Spark.ipynb : exploring provided files and demsontrating data pipeline
- loadtomysql.ipynb: load csv to MySQL database
- report.ipynb: reports from loaded data using sql and python 


## Python scripts
- etl.py: Read JSON logs and JSON metadata and turned them into CSV.

### Steps:
Have all provided files in `Data` folder.
`python etl.py`
Process data from `Data` folder with etl pipeline that will create valid rows in CSV files in `Result` folder, malformed records in `malformed.log` and `error.log`. 

Run `loadtomysql.ipynb`.
Read csv from `Result` folder and insert data into each event table. This can be automated to py file as well. 

Run `report.ipnb`.
Loop through all three files source and query result into report.

If given more time:
- Valid rows can be directly inserted to database with python scripts. 
- In real world application, data can be streamed with the py file. 
