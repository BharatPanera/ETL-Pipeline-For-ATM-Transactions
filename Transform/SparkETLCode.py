from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pysparkForETL').master("local").getOrCreate()
spark

# Load the data from HDFS to DataFrame API
atm_data = spark.read.csv('/user/root/SRC_ATM_TRANS/part-m-00000', header=False, inferSchema=True)

# view the data
atm_data.show(3)

atm_data.columns

# Import classed from pyspark.sql.types module
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType, LongType

# Create custume schema using StrucType
Schema = StructType([StructField('year', IntegerType(), nullable = True),
                        StructField('month', StringType(), True),
                        StructField('day', IntegerType(), True),
                        StructField('weekday', StringType(), True),
                        StructField('hour', IntegerType(), True),
                        StructField('atm_status', StringType(), True),
                        StructField('atm_id', StringType(), True),
                        StructField('atm_manufacturer', StringType(), True),
                        StructField('atm_location', StringType(), True),
                        StructField('atm_streetname', StringType(), True),
                        StructField('atm_street_number', IntegerType(), True),
                        StructField('atm_zipcode', IntegerType(), True),
                        StructField('atm_lat', DoubleType(), True),
                        StructField('atm_lon', DoubleType(), True),
                        StructField('currency', StringType(), True),
                        StructField('card_type', StringType(), True),
                        StructField('transaction_amount', IntegerType(), True),
                        StructField('service', StringType(), True),
                        StructField('message_code', StringType(), True),
                        StructField('message_text', StringType(), True),
                        StructField('weather_lat', DoubleType(), True),
                        StructField('weather_lon', DoubleType(), True),
                        StructField('weather_city_id', IntegerType(), True),
                        StructField('weather_city_name', StringType(), True),
                        StructField('temp', DoubleType(), True),
                        StructField('pressure', IntegerType(), True),
                        StructField('humidity', IntegerType(), True),
                        StructField('wind_speed', IntegerType(), True),
                        StructField('wind_deg', IntegerType(), True),
                        StructField('rain_3h', DoubleType(), True),
                        StructField('clouds_all', IntegerType(), True),
                        StructField('weather_id', IntegerType(), True),
                        StructField('weather_main', StringType(), True),
                        StructField('weather_description', StringType(), True)])


# Load data again to check the changed schema
atm_data = spark.read.csv('/user/root/SRC_ATM_TRANS/part-m-00000', header=False, schema=Schema)

# Print the schema fields to varify the changed schema
atm_data.printSchema()

# Count the data records loaded into DataFrame
atm_data.select('*').count()

"""
# Create fact and dimension tables
"""

"""
#### 1. Create dimension - DIM_LOCATION
"""

# Filter columns that are required for the location dimension
location_dim_temp = atm_data.select('atm_location', 'atm_streetname', 'atm_street_number', 'atm_zipcode', 'atm_lat', 'atm_lon').distinct()
location_dim_temp.show(5)

# import necessary classes and functions from PySpark
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# Create a primary key('location_id') in location dimension as required
location_dim_temp1 = location_dim_temp.select(row_number().over(Window.orderBy(location_dim_temp[0])).alias("location_id"), "*")
location_dim_temp1.show(5)

# Rename the column name of the loction dimension as per requirement
DIM_LOCATION = location_dim_temp1.withColumnRenamed('atm_location','location')\
                            .withColumnRenamed('atm_streetname','streetname')\
                            .withColumnRenamed('atm_street_number','street_number')\
                            .withColumnRenamed('atm_zipcode','zipcode')\
                            .withColumnRenamed('atm_lat','lat')\
                            .withColumnRenamed('atm_lon','lon')
DIM_LOCATION.show(5)


"""
#### 2. Create dimension - DIM_ATM
"""

# Filter columns that are required for the ATM dimension
atm_dim_temp = atm_data.select(col('atm_id').alias('atm_number'), 'atm_manufacturer', 'atm_lat', 'atm_lon')
atm_dim_temp.show(5)

atm_dim_temp = atm_dim_temp.join(location_dim_temp1, on = ['atm_lat', 'atm_lon'], how = "left")
atm_dim_temp.columns

# Filter only required columns and keep the record distinct
atm_dim_temp1 = atm_dim_temp.select('atm_number', 'atm_manufacturer', 'location_id').distinct()
atm_dim_temp1.columns

DIM_ATM = atm_dim_temp1.select(row_number().over(Window.orderBy(atm_dim_temp1[0])).alias('atm_id'),
                               'atm_number', 'atm_manufacturer', col('location_id').alias('atm_location_id'))
DIM_ATM.show(5)


"""
#### 3. Create dimension - DIM_DATE
"""

# Filter columns that are required for the date dimension
date_dim_temp = atm_data.select('year', 'month', 'day', 'hour', 'weekday')
date_dim_temp.show(5)

# Create columns full_date_time as per requirement
date_dim_temp = date_dim_temp.withColumn('full_date_time', to_timestamp(concat(date_dim_temp.year, lit('-'), 
                                                                               date_dim_temp.month, lit('-'),
                                                                               date_dim_temp.day, lit(' '), 
                                                                               date_dim_temp.hour), 'yyyy-MMMM-d H'))
date_dim_temp.show(5)

# Filter only required columns and keep the record distinct
date_dim_temp = date_dim_temp.select('full_date_time', 'year', 'month', 'day', 'hour', 'weekday').distinct()

# Craete a col 'date_id' as per requirements
DIM_DATE = date_dim_temp.select(row_number().over(Window.orderBy(date_dim_temp[0])).alias('date_id'), '*')
DIM_DATE.show(5)


"""
#### 4. Create dimension - DIM_CARD_TYPE
"""

# Filter columns that are required for the card_type dimension
card_dim_temp =  atm_data.select('card_type').distinct()

# Create a primary key for card_type dimension
DIM_CARD_TYPE = card_dim_temp.select(row_number().over(Window.orderBy(card_dim_temp[0])).alias("card_type_id"), "*")
DIM_CARD_TYPE.show(5)


"""
### Create fact table:
"""

"""
#### 1. Create fact table - FACT_ATM_TRANS
"""

# Filter the required cols from the originam DataFrame and rename it
# The original column names are renamed to more general names to match the target schema.
fact_location_temp = atm_data.withColumnRenamed('atm_location','location')\
                             .withColumnRenamed('atm_streetname','streetname')\
                             .withColumnRenamed('atm_street_number','street_number')\
                             .withColumnRenamed('atm_zipcode','zipcode')\
                             .withColumnRenamed('atm_lat','lat')\
                             .withColumnRenamed('atm_lon','lon')

# Join the DataFrame with dimension
fact_location_temp = fact_location_temp.join(DIM_LOCATION, on = ['location', 'streetname', 'street_number', 'zipcode', 'lat', 'lon'], how = "left")
fact_location_temp = fact_location_temp.withColumnRenamed('atm_id', 'atm_number').withColumnRenamed('location_id', 'atm_location_id')

# Join the DataFrame for the 'DIM_ATM'
fact_atm_temp = fact_location_temp.join(DIM_ATM, on = ['atm_number', 'atm_manufacturer', 'atm_location_id'], how = "left")
fact_atm_temp = fact_atm_temp.withColumnRenamed('atm_location_id', 'weather_loc_id')

# Join the DataFrame for the 'DIM_DATE'
fact_date_temp = fact_atm_temp.join(DIM_DATE, on = ['year', 'month', 'day', 'hour', 'weekday'], how = "left")

# Joint the DataFrame with the 'DIM_CARD_TYPE' dimension
fact_atm_trans_temp = fact_date_temp.join(DIM_CARD_TYPE, on = ['card_type'], how = "left")

# Create primary key ('trans_id') in fact table as per requirements
FACT_ATM_TRANS = fact_atm_trans_temp.withColumn("trans_id", row_number().over(Window.orderBy('date_id')))
FACT_ATM_TRANS.columns

# Rearrange the columns in fact table as per the target model
FACT_ATM_TRANS = FACT_ATM_TRANS.select('trans_id', 'atm_id', 'weather_loc_id', 'date_id', 'card_type_id', 'atm_status',
                                       'currency', 'service', 'transaction_amount', 'message_code', 'message_text', 'rain_3h', 
                                       'clouds_all', 'weather_id', 'weather_main', 'weather_description')
FACT_ATM_TRANS.show(5)

# varify the count of the fact table
FACT_ATM_TRANS.count()


"""
# Export the final data to S3 bucket
"""

def upload_to_s3(data_frame, s3_file_name):
    """
    Save a PySpark DataFrame to an S3 bucket in CSV format.

    Parameters:
    data_frame (DataFrame): The PySpark DataFrame to be saved.
    s3_file_name (str): The S3 file path including the file name where the DataFrame should be saved.
    """
    try:
        data_frame.write.format('csv').option('header', 'false').save(f's3://etl-atm-data/{s3_file_name}', mode='overwrite')
        print(f'DataFrame successfully saved to s3://etl-atm-data/{s3_file_name}')
    except Exception as e:
        print(f'An error occurred while saving the DataFrame to S3: {e}')


upload_to_s3(DIM_LOCATION, 'dim_location')
upload_to_s3(DIM_ATM, 'dim_atm')
upload_to_s3(DIM_DATE, 'dim_date')
upload_to_s3(DIM_CARD_TYPE, 'dim_card_type')
upload_to_s3(FACT_ATM_TRANS, 'fact_atm_trans')