# Udacity Data Engineering capstone project

For this final Data Engineering project I'm completing a capstone project.

The steps listed in the exercise have been documented in the file Capstone_Project.ipynb.

1. Scope the Project and Gather Data
2. Explore and Assess the Data
3. Define the Data Model
4. Run ETL to Model the Data
5. Complete Project Write Up

The etl.py file reads the staging data from S3, creates fact/dimension tables and writes it back to S3.
Processing is done on an AWS EMR cluster.

# Prerequisites
I have read in the sas data in the Udacity workspace, using the specified functionality.
This sas data has been converted to parquet and has been copied to my s3://jjudacitydatalake/staging folder as i94_parquet data.




