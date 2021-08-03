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

sasdate_udf = udf(sasDateToDatetime, t.DateType())

def readMultipleParquet(displayName, listPaths):
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


def read_immigration_staging(listPaths):
    """
    Given a list of strings representing paths to sas data files stored in parquet format, read and transform the immmigration staging data and return the DataFrame.
    """
    
    spark.sparkContext.setJobGroup("Read", "Read raw immigration staging data")

    raw_data = readMultipleParquet("Reading multiple parquet files", listPaths)

    print(f"Number of raw rows read: {raw_data.count()}")
    
    spark.sparkContext.setJobGroup("Read", "Read and transform immigration staging data")
    
    final_data = raw_data.\
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

    print(f"Number of rows in final selected dataset: {final_data.count()}")
    
    return raw_data, final_data

def read_temperature_staging(listPaths):
    """
    Given a list of paths to csv files, read in the temperature data, run transform and return dataframe.
    """

    spark.sparkContext.setJobGroup("Read", "Read raw temperature staging data")
    raw_data = spark.read.option("header", "true").csv(*listPaths)

    spark.sparkContext.setJobGroup("Read", "Read and transform temperature staging data")
    final_data = raw_data.\
    filter(col('Country') == 'United States').\
    select(to_date(col("dt"),"yyyy-MM-dd").alias("dt"), 'AverageTemperature', 'City', 'Country', 'Latitude', 'Longitude').\
    withColumn('dayofmonth', dayofmonth(col('dt'))).\
    withColumn('month', month(col('dt'))).\
    withColumn('year', year(col('dt'))).\
    withColumn("latitude_rounded", format_string("%.0f", regexp_extract(col('Latitude'), '\d+.\d+', 0).cast(t.DoubleType()))).\
    withColumn("longitude_rounded", format_string("%.0f", regexp_extract(col('Longitude'), '\d+.\d+', 0).cast(t.DoubleType()))).\
    dropna()
    
    return raw_data, final_data    

def read_airport_codes_staging(listPaths):
    """
    Given a list of strings to airport code csv files, return the raw and final dataframes of airport codes.
    """
    
    spark.sparkContext.setJobGroup("Read", "Read raw airport code staging data")
    raw_data = spark.read.option("header", "true").csv(*listPaths)
    
    # coordinates are specified in [longitude, latitude]
    coordinates_split = split(raw_data['coordinates'], ',')
    region_split = split(raw_data['iso_region'], '-')

    spark.sparkContext.setJobGroup("Read", "Read and transform airport code staging data")
    final_data = raw_data.\
                        filter(col('iso_country') == 'US').\
                        withColumn("latitude", format_string("%.0f", abs(coordinates_split.getItem(1).cast(t.DoubleType())))).\
                        withColumn("longitude", format_string("%.0f", abs(coordinates_split.getItem(0).cast(t.DoubleType())))).\
                        withColumn("state", region_split.getItem(1)).\
                        withColumn('state', when(~col('state').isin(valid_us_states), 'other').otherwise(col('state'))).\
                        fillna('other', subset='state')
    
    return raw_data, final_data    

def create_temperature_table():
    """
    Given two dataframes, airport_codes_final_data and temperature_final_data, join these on coordinates and return a dataframe
    with columns day of month, month, state and the average temperature applicable
    """
    spark.sparkContext.setJobGroup("Transform", "Create temperature helper table")
    
    temperature_final_data.createOrReplaceTempView("tempdata_coord")
    temp_table = spark.sql("""
    select dayofmonth, month, latitude_rounded as lat, longitude_rounded as long, avg(AverageTemperature) as AvgTemp
    from tempdata_coord
    group by lat, long, month, dayofmonth
    order by lat asc, long asc, month asc, dayofmonth asc
    """)

    airport_codes_final_data.createOrReplaceTempView("aircodes")
    # count the number of states for each lat/long pair
    aircode_table1 = spark.sql("""
    select latitude, longitude, state, count(state) as num
    from aircodes
    group by latitude, longitude, state
    order by latitude, longitude, state
    """)

    airport_codes_final_data.createOrReplaceTempView("aircodes")
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
    aircode_table3 = aircode_table1.\
        join(aircode_table2, [aircode_table1.latitude == aircode_table2.lat, aircode_table1.longitude == aircode_table2.long, aircode_table1.num == aircode_table2.maxPerLatLong]).\
        drop('long', 'lat', 'num', 'maxPerLatLong')

    # finally, join both together on coordinates
    state_temp = temp_table.join(aircode_table3, [temp_table.lat == aircode_table3.latitude, temp_table.long == aircode_table3.longitude])

    state_temp.createOrReplaceTempView("state_temp")
    state_temp2 = spark.sql("""
    select dayofmonth, month, state, avg(AvgTemp) as avg_temp
    from state_temp
    group by dayofmonth, month, state
    order by dayofmonth, month, state
    """)
    
    state_temp2 = state_temp2.withColumn("id_temp", monotonically_increasing_id())
    
    return state_temp2

def create_dim_state():
    """
    Arguments: none. Return state dataframe.
    """
    spark.sparkContext.setJobGroup("Read", "Read and transform dim_state")
    
    return immigration_final_data.\
            select('state').\
            distinct().\
            withColumn("id_state", monotonically_increasing_id())
    
def create_dim_time():
    """
    Arguments: none. Return time dataframe.
    """
    spark.sparkContext.setJobGroup("Read", "Read and transform dim_time")
    return immigration_final_data.\
            select(col('arrdate_dt').alias('date'), col('arrdate_dayofmonth').alias('day_of_month'), col('arrdate_month').alias('month'), col('arrdate_year').alias('year')).\
            distinct().\
            withColumn("id_time", monotonically_increasing_id())
    
def create_dim_person():
    """
    Arguments: none. Return person dataframe.
    """    
    spark.sparkContext.setJobGroup("Read", "Read and transform dim_person")
    return immigration_final_data.\
            select('gender', 'biryear', 'id_imm').\
            withColumn("id_person", monotonically_increasing_id())
    
def create_dim_ports():
    """
    Arguments: none. Return ports dataframe.
    """    
    spark.sparkContext.setJobGroup("Read", "Read and transform dim_ports")
    return immigration_final_data.\
            select('i94port').alias('port').\
            distinct().\
            withColumn("id_port", monotonically_increasing_id())
    
def create_dim_airlines():
    """
    Arguments: none. Return airline dataframe.
    """    
    spark.sparkContext.setJobGroup("Read", "Read and transform dim_airlines")
    return immigration_final_data.\
            select('airline').\
            distinct().\
            withColumn("id_airline", monotonically_increasing_id())
    
def create_fact_temp():
    """
    Arguments: none. Return temperature dataframe.
    """    
    spark.sparkContext.setJobGroup("Read", "Read and transform dim_temp")
    
    return state_temp

def create_fact_imm():
    """
    Arguments: none. Return fact_imm dataframe.
    """    
    spark.sparkContext.setJobGroup("Read", "Read and transform fact_imm")
    
    # we specifically perform left joins. we could transition to inner joins but some of the tables incomplete due to lacking information in the temperature/airport code table.
    return immigration_final_data.\
            join(dim_time, [immigration_final_data.arrdate_dt == dim_time.date], "left").\
            join(dim_airlines, [immigration_final_data.airline == dim_airlines.airline], "left").\
            join(dim_ports, [immigration_final_data.i94port == dim_ports.i94port], "left").\
            join(dim_state, [immigration_final_data.state == dim_state.state], "left").\
            join(fact_temp, [immigration_final_data.arrdate_dayofmonth == fact_temp.dayofmonth, immigration_final_data.arrdate_month == fact_temp.month, immigration_final_data.state == fact_temp.state], "left").\
            join(dim_person, [immigration_final_data.id_imm == dim_person.id_imm], "left").\
            select(immigration_final_data.id_imm, dim_state.id_state, 'id_time', 'id_person', 'id_port', 'id_airline', 'id_temp')


if __name__ == "__main__":
    # set up immigration data
    immigration_raw_data, immigration_final_data = read_immigration_staging([f's3://{s3_bucket}/capstone/staging/i94_parquet/i94_apr16_sub.sas7bdat', f's3://{s3_bucket}/capstone/staging/i94_parquet/i94_may16_sub.sas7bdat'])

    # set up temperature data
    temperature_raw_data, temperature_final_data = read_temperature_staging([f's3://{s3_bucket}/capstone/staging/temperature_data/GlobalLandTemperaturesByCity.csv'])

    # set up airport code data
    airport_codes_raw_data, airport_codes_final_data = read_airport_codes_staging([f's3://{s3_bucket}/capstone/staging/airportcodes_data/airport-codes_csv.csv'])

    # create temperature table
    state_temp = create_temperature_table()

    # set up dimension tables
    dim_state = create_dim_state()
    dim_state.persist()
    dim_time = create_dim_time()
    dim_time.persist()
    dim_person = create_dim_person()
    dim_person.persist()
    dim_ports = create_dim_ports()
    dim_ports.persist()
    dim_airlines = create_dim_airlines()
    dim_airlines.persist()

    # create fact tables
    fact_temp = create_fact_temp()
    fact_imm = create_fact_imm()

    # write out dimension/fact tables to s3
    writeToS3('dim_state', f"s3a://{s3_bucket}/capstone/processed/dim_state", 'overwrite', 'parquet', dim_state)
    writeToS3('dim_time', f"s3a://{s3_bucket}/capstone/processed/dim_time", 'overwrite', 'parquet', dim_time)
    writeToS3('dim_person', f"s3a://{s3_bucket}/capstone/processed/dim_person", 'overwrite', 'parquet', dim_person)
    writeToS3('dim_airlines', f"s3a://{s3_bucket}/capstone/processed/dim_airlines", 'overwrite', 'parquet', dim_airlines)
    writeToS3('dim_ports', f"s3a://{s3_bucket}/capstone/processed/dim_ports", 'overwrite', 'parquet', dim_ports)

    writeToS3('fact_imm', f"s3a://{s3_bucket}/capstone/processed/fact_imm", 'overwrite', 'parquet', fact_imm)
    writeToS3('fact_temp', f"s3a://{s3_bucket}/capstone/processed/fact_temp", 'overwrite', 'parquet', fact_temp)

    # perform data quality checks
    spark.sparkContext.setJobGroup("DataQuality", "Counting number of records in tables")
    
    expectedRowCount = {dim_state : 52, 
                        fact_temp : 444, 
                        dim_time : 61, 
                        fact_imm : 5388905,
                        dim_airlines : 622,
                        dim_person : 5388905,
                        dim_ports : 314,}
                            
    for obj in [dim_state, fact_temp, dim_time, fact_imm, dim_airlines, dim_person, dim_ports]:
        print(f"Evaluating {obj}")
        logging.info(f"Evaluating {obj}")        
        numRows = recordCount(obj)
        
        checkNumberOfRows(numRows, expectedRowCount[obj])

    spark.sparkContext.setJobGroup("DataQuality", "Counting total number of distinct states")
    dim_state.createOrReplaceTempView("state")
    numDistinctStates = spark.sql("""
    select count(distinct state) 
    from state
    """)

    checkNumberOfRows(numDistinctStates.collect()[0]['count(DISTINCT state)'], len(valid_us_states) + 1)

    # and finally, close spark nicely
    spark.stop()