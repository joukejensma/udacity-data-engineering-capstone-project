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

# Responding to reviewer comments
After my first submission I have gotten a few requests for clarification, such as:

> But please provide information on the purpose of the final data model, such as How the users use this data model to get the tourism insights.

I have added some extra information in the Notebook. The gist of it is that the processed star schema is written to S3. Users wanting to perform an analysis will be able to read in the parquet files on S3 and use the processed tables for their own analysis purposes. See the example below in the notebook for a query an analyst might run.

An optional encouragement was added as well:
> But I would encourage you to think a little more, for example, when the data was increased by 100x, do you store the data in the same way with Spark? 

This question and others have been expanded upon in the Notebook (see Step 5, project write up).

> But please provide the required justification about this schema, such as why the tables are linked in this way, what could be the index of each table, and what could be the example queries users will use.

To gain insight I have added the database schema (using the dbdiagram.io tool, thanks for the suggestion!). I have normalized the tables in such a way that queries will run efficiently. An example query is given below in the Notebook.

