# ETL-Pipeline-For-ATM-Transactions

## Project Overview

Spar Nord Bank aims to observe withdrawal behavior and the corresponding dependent factors to optimally manage the refill frequency of ATMs. The project also aims to derive other valuable insights from the data. Our task is to build a batch ETL pipeline to read transactional data from RDS, transform and load it into target dimensions and facts on Redshift Data Mart (Schema), and then perform various analytical queries.

## Problem Statement

In this project, we will address the following analytical queries:

1. Top 10 ATMs where most transactions are in the ‘inactive’ state
2. Number of ATM failures corresponding to the different weather conditions recorded at the time of the transactions
3. Top 10 ATMs with the most number of transactions throughout the year
4. Number of overall ATM transactions going inactive per month for each month
5. Top 10 ATMs with the highest total amount withdrawn throughout the year
6. Number of failed ATM transactions across various card types
7. Top 10 records with the number of transactions ordered by the ATM_number, ATM_manufacturer, location, weekend_flag and then total_transaction_count, on weekdays and on weekends throughout the year
8. Most active day in each ATMs from location "Vejgaard"

## Project Workflow

### 1. Data Extraction

**Source:** MySQL RDS Server  
**Destination:** HDFS (EC2 Instance)  
**Tool:** Sqoop

#### Steps:
- Extract transactional data from the MySQL RDS server using Sqoop.
- Import the data into HDFS.

### 2. Data Transformation

**Tool:** PySpark

#### Steps:
- Read data from HDFS using a specific schema.
- Create input schema using `StructType` in PySpark.
- Clean and transform the data:
  - Remove duplicate records.
  - Ensure appropriate primary keys for dimensions.
  - Rearrange fields as necessary.

#### Dimensions and Fact Table:
1. **ATM Dimension** - Contains data related to various ATMs including ATM number, manufacturer, and location reference.
2. **Location Dimension** - Contains location data including location name, street name, street number, zip code, latitude, and longitude.
3. **Date Dimension** - Contains data related to transaction date and time including year, month, day, hour, and weekday.
4. **Card Type Dimension** - Contains information about different card types.
5. **Transaction Fact** - Contains all numerical data such as transaction amount, weather info, and ATM status.

### 3. Data Loading to S3

#### Steps:
- Write transformed dimension and fact tables to an S3 bucket.

### 4. Data Loading to Redshift

**Tool:** Amazon Redshift

#### Steps:
- Create a Redshift cluster.
- Set up a database and create dimension and fact tables with appropriate primary and foreign keys.
- Load data from S3 into Redshift tables.

### 5. Analytical Queries

**Tool:** Amazon Redshift

#### Steps:
- Perform the following analytical queries on the Redshift cluster:
  1. Top 10 ATMs where most transactions are in the ‘inactive’ state.
  2. Number of ATM failures corresponding to the different weather conditions recorded at the time of the transactions.
  3. Top 10 ATMs with the most number of transactions throughout the year.
  4. Number of overall ATM transactions going inactive per month for each month.
  5. Top 10 ATMs with the highest total amount withdrawn throughout the year.
  6. Number of failed ATM transactions across various card types.
  7. Top 10 records with the number of transactions ordered by ATM_number, ATM_manufacturer, location, weekend_flag, and total_transaction_count on weekdays and weekends throughout the year.
  8. Most active day in each ATMs from location "Vejgaard".

## Dataset Description

The dataset comprises around 2.5 million records of withdrawal data along with weather information from approximately 113 ATMs across Denmark in 2017. It includes:
- **Transaction Data:** Year, month, day, weekday, hour, ATM ID, manufacturer name, location details (longitude, latitude, street name, street number, zip code).
- **Weather Data:** Weather type, temperature, pressure, wind speed, cloud cover, etc.
- **Transaction Details:** Card type, currency, transaction/service type, transaction amount, error message (if any).

## Target Schema

### Dimensions
1. **ATM Dimension**
   - ATM Number (ATM ID)
   - Manufacturer
   - Location Reference

2. **Location Dimension**
   - Location Name
   - Street Name
   - Street Number
   - Zip Code
   - Latitude
   - Longitude

3. **Date Dimension**
   - Full Date and Time Timestamp
   - Year
   - Month
   - Day
   - Hour
   - Weekday

4. **Card Type Dimension**
   - Card Type

### Fact Table
- Transaction Amount
- Weather Info
- ATM Status

## Tools and Technologies

- **Data Extraction:** RDS, Sqoop, HDFS, EMR
- **Data Transformation:** PySpark
- **Data Storage:** Amazon S3
- **Data Warehouse:** Amazon Redshift

## Conclusion

This project helps Spar Nord Bank to optimize the ATM refill frequency and understand various factors affecting ATM transactions. By building an efficient ETL pipeline and performing the analytical queries, we derive insights that can aid in better decision-making for ATM management.

---

Please feel free to explore the repository for detailed scripts and additional information.

---
