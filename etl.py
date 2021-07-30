from pyspark.sql.functions import udf, to_date, col, month, year, dayofmonth, split, format_string, abs, isnan, when, count, substring, length, regexp_extract, monotonically_increasing_id
from pyspark.sql import SparkSession
from datetime import datetime
from datetime import timedelta

import logging
import pyspark.sql.types as t
import numpy as np
import os
import configparser

config = configparser.ConfigParser()
config.read('settings.cfg')

# ensure that the environment variables are set before the spark session is started, otherwise s3 cannot be accessed
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
s3_bucket=config['AWS']['AWS_S3_BUCKET_LOC']

# list of valid US states, these will be contained in the output (or 'other' if no match)
valid_us_states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

# first off, start spark
spark = SparkSession.builder.appName("Capstone Project").getOrCreate()

def writeToS3(displayName, destPath, writeMode, fmt, dataFrame):
    """
    Given a display name displayName, write dataFrame to s3 path 'destPath' with writeMode either 'append' or overwrite
    Use format fmt
    Returns None 
    """

    logging.info(f"Writing {displayName} to {destPath} with format {fmt}, mode {writeMode}.")

    dataFrame.write.format(fmt).mode(writeMode).save(destPath)

def sasDateToDatetime(sasdate):
    """
    Given a spark column which specifies the number of days since 1960, return the datetime object
    """
    return None if sasdate == None else datetime.strptime('1960-01-01', "%Y-%m-%d") + timedelta(sasdate)

def readParquet(displayName, listPaths):
    """
    Given a list of paths to parquet files, return Spark Dataframe for processing.
    Show displayName for output.
    """
    spark.sparkContext.setJobGroup("Read", f"Reading multiple parquet files ({displayName})")

    df = spark.read.parquet(*listPaths)

    logging.info(f"Number of rows read (for {displayName}): {df.count()}")
    return df

def readCsv(displayName, listPaths, header=True):
    """
    Given a list of paths to csv files, return Spark DataFrame for processing.
    Show displayName for output.
    """
    if header:
        df = spark.read.option("header", "true").csv(*listPaths)
    else:
        df = spark.read.option("header", "false").csv(*listPaths)

    logging.info(f"Number of rows read (for {displayName}): {df.count()}")
    return df        

def recordCount(table_name):
    """
    Given a dataframe table_name, return the number of records in it
    """
    return table_name.count()

def checkNumberOfRows(actual_count, expected_count):
    """
    Given a number actual_count, compare to expected_count and raise ValueError exception if it differs.
    """
    if actual_count != expected_count:
        logging.error(f"The number of records found is {actual_count}, differing from expected value {expected_count}")
        raise ValueError(f"The number of records found is {actual_count}, differing from expected value {expected_count}")


if __name__ == "__main__":
    listImmStagingPaths = [f's3://{s3_bucket}/capstone/staging/i94_parquet/i94_apr16_sub.sas7bdat', f's3://{s3_bucket}/capstone/staging/i94_parquet/i94_may16_sub.sas7bdat']
    immigration_staging = readParquet("Immigration staging data", listImmStagingPaths)
    
    # set up immigration data
    sasdate_udf = udf(sasDateToDatetime, t.DateType())
    imm = immigration_staging.\
        withColumn('arrdate_dt', sasdate_udf('arrdate')).\
        withColumn('depdate_dt', sasdate_udf('depdate')).\
        withColumn('arrdate_dayofmonth', dayofmonth(col('arrdate_dt'))).\
        withColumn('arrdate_month', month(col('arrdate_dt'))).\
        withColumn('arrdate_year', year(col('arrdate_dt'))).\
        withColumn('state', when(~col('i94addr').isin(valid_us_states), 'other').otherwise(col('i94addr'))).\
        fillna('other', subset='state').\
        fillna('unknown', subset='gender').\
        dropDuplicates().\
        select('i94port', 'biryear', 'gender', 'airline', 'i94visa', 'arrdate_dt', 'depdate_dt', 'arrdate_dayofmonth', 'arrdate_month', 'arrdate_year', 'state').\
        filter(col('i94visa') == 2).\
        withColumn('id_imm', monotonically_increasing_id())

    temp_staging = readCsv("Temperature data", [f's3://{s3_bucket}/capstone/staging/temperature_data/GlobalLandTemperaturesByCity.csv'])

    # set up temperature data
    df_temp = temp_staging.\
        filter(col('Country') == 'United States').\
        select(to_date(col("dt"),"yyyy-MM-dd").alias("dt"), 'AverageTemperature', 'City', 'Country', 'Latitude', 'Longitude').\
        withColumn('dayofmonth', dayofmonth(col('dt'))).\
        withColumn('month', month(col('dt'))).\
        withColumn('year', year(col('dt'))).\
        withColumn("latitude_rounded", format_string("%.0f", regexp_extract(col('Latitude'), '\d+.\d+', 0).cast(t.DoubleType()))).\
        withColumn("longitude_rounded", format_string("%.0f", regexp_extract(col('Longitude'), '\d+.\d+', 0).cast(t.DoubleType()))).\
        dropna()
    
    df_temp.createOrReplaceTempView("tempdata_coord")
    temp_table = spark.sql("""
    select dayofmonth, month, latitude_rounded as lat, longitude_rounded as long, avg(AverageTemperature) as AvgTemp
    from tempdata_coord
    group by lat, long, month, dayofmonth
    order by lat asc, long asc, month asc, dayofmonth asc
    """)

    temp_table = temp_table.withColumn("id_temp_coord", monotonically_increasing_id())

    # set up airport code data
    airport_codes_staging = readCsv("Airport code data", [f's3://{s3_bucket}/capstone/staging/airportcodes_data/airport-codes_csv.csv'])

    # coordinates are specified in [longitude, latitude]
    coordinates_split = split(airport_codes_staging['coordinates'], ',')
    region_split = split(airport_codes_staging['iso_region'], '-')

    df_airportcodes = airport_codes_staging.\
                        filter(col('iso_country') == 'US').\
                        withColumn("latitude", format_string("%.0f", abs(coordinates_split.getItem(1).cast(t.DoubleType())))).\
                        withColumn("longitude", format_string("%.0f", abs(coordinates_split.getItem(0).cast(t.DoubleType())))).\
                        withColumn("state", region_split.getItem(1)).\
                        withColumn('state', when(~col('state').isin(valid_us_states), 'other').otherwise(col('state'))).\
                        fillna('other', subset='state')

    df_airportcodes.createOrReplaceTempView("aircodes")
    # count the number of states for each lat/long pair
    aircode_table1 = spark.sql("""
    select latitude, longitude, state, count(state) as num
    from aircodes
    group by latitude, longitude, state
    order by latitude, longitude, state
    """)

    # determine the maximum count per lat/long pair
    aircode_table2 = spark.sql("""
    select latitude as lat, longitude as long, max(num) as maxPerLatLong from (
        select latitude, longitude, state, count(state) as num
        from aircodes
        group by latitude, longitude, state
        order by latitude, longitude, state
    )
    group by lat, long
    order by lat, long
    """)

    # join both tables to get the state with the most counts for each lat/long pairs
    aircode_table = aircode_table1.\
        join(aircode_table2, [aircode_table1.latitude == aircode_table2.lat, aircode_table1.longitude == aircode_table2.long, aircode_table1.num == aircode_table2.maxPerLatLong]).\
        drop('long', 'lat', 'num', 'maxPerLatLong').\
        withColumn("id_state_coord", monotonically_increasing_id())

    # set up dimension tables
    dim_state_coord = aircode_table.persist()

    dim_temp_coord = temp_table.persist()

    imm.createOrReplaceTempView("immdata")
    dim_time = spark.sql("""
    select distinct arrdate_dt as datetime, arrdate_dayofmonth as dayofmonth, arrdate_month as month, arrdate_year as year
    from immdata
    """).persist()

    dim_imm = imm

    # set up fact table
    fact_imm = dim_imm.\
        join(dim_time, [dim_imm.arrdate_dt == dim_time.datetime]).\
        join(dim_temp_coord, [dim_imm.arrdate_dayofmonth == dim_temp_coord.dayofmonth, dim_imm.arrdate_month == dim_temp_coord.month]).\
        join(dim_state_coord, [dim_temp_coord.lat == dim_state_coord.latitude, dim_temp_coord.long == dim_state_coord.longitude]).\
        select('id_imm', 'id_state_coord', 'id_temp_coord', 'datetime').\
        dropDuplicates()

    # write out dimension/fact tables to s3
    writeToS3('dim_state_coord', f"s3a://{s3_bucket}/capstone/processed/dim_state_coord", 'overwrite', 'parquet', dim_state_coord)
    writeToS3('dim_temp_coord', f"s3a://{s3_bucket}/capstone/processed/dim_temp_coord", 'overwrite', 'parquet', dim_temp_coord)
    writeToS3('dim_time', f"s3a://{s3_bucket}/capstone/processed/dim_time", 'overwrite', 'parquet', dim_time)
    writeToS3('dim_imm', f"s3a://{s3_bucket}/capstone/processed/dim_imm", 'overwrite', 'parquet', dim_imm)
    writeToS3('fact_imm', f"s3a://{s3_bucket}/capstone/processed/fact_imm", 'overwrite', 'parquet', fact_imm)

    # perform data quality checks
    spark.sparkContext.setJobGroup("DataQuality", "Counting number of records in tables")

    expectedRowCount = {dim_state_coord : 1200, dim_temp_coord : 1164, dim_time : 61, dim_imm : 5388905, fact_imm : 17445547}
                            
    for obj in [dim_state_coord, dim_temp_coord, dim_time, dim_imm, fact_imm]:
        numRows = recordCount(obj)
        
        checkNumberOfRows(numRows, expectedRowCount[obj])

    spark.sparkContext.setJobGroup("DataQuality", "Counting total number of distinct states")
    numDistinctStates = spark.sql("""
    select count(distinct state) 
    from immdata
    """)

    checkNumberOfRows(numDistinctStates.collect()[0]['count(DISTINCT state)'], len(valid_us_states) + 1)

    spark.stop()