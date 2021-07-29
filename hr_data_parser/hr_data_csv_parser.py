# Standard library modules
import logging
import os
import pandas as pd
import awswrangler as wr
import boto3
import sys
import json
import numpy as np
from ast import literal_eval
from datetime import datetime

# Local reusable modules
from hr_data_csv_schema import ( read_schema,
                                 date_columns_schema,
                                 json_columns_schema,
                                 custom_data_schema,
                                 write_schema )

# Amazon Simple Storage Service (S3)
s3 = boto3.client('s3')

# Event logging
logger = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.INFO)

'''
Helper functions
'''

def write_to_s3(df, target_path):
    """
    Write pandas dataframe data as parquet file(s) on Amazon S3.
    :param df: Pandas DataFrame name
    :param target path: full target path to store data
    :return: Dictionary with the following information:
        ‘paths’: List of all stored files paths on S3.
        ‘partitions_values’: Dictionary of partitions added with keys as S3 path locations and values as a list of partitions values as str.
    """

    try:
        stored_files = wr.s3.to_parquet(
            df=df,
            path=target_path,
            index=False,
            compression="snappy",
            use_threads=False,
            dataset=True,
            mode="overwrite"
        )
    except Exception as e:
        raise(e)

    logging.info("stored_files {}".format(stored_files))
    return stored_files

def read_hr_data_csv(file_path, file_schema, date_cols):
    """
    Extract HR Data column values from the CSV file
    :param file_path: HR Data file path on the S3
    :param file_schema: CSV schema
    :param date_cols: date columns to parse
    :return: returns raw_df pandas dataframe
    """

    # Read HR Data CSV on S3 using pandas dataframe
    raw_df = wr.s3.read_csv(path=file_path, dtype=file_schema, parse_dates=date_cols)

    # Return pandas dataframe
    return raw_df

def get_file_name(ib_bucket_name, ib_bucket_prefix):
    """
    Extract HR Data CSV file name from source path
    :param ib_bucket_name: HR Data S3 bucket name
    :param ib_bucket_prefix: HR Data CSV file path prefix on S3
    :return: returns file name as a string
    """
    try:
        result = s3.list_objects_v2(Bucket=ib_bucket_name, Prefix=ib_bucket_prefix)
        result_list = result['Contents'][0]['Key'].split("/")

        # Return file name based on source file path
        return result_list[-1]
    except:
        return None

def get_source_file_date_time(file_name):
    """
    Extract HR Data CSV file date from file name
    :param file_name: HR Data CSV file name on S3
    :return: returns file date as timestamp
    """
    try:
        # Split the file name based on .csv and _ to extract file name elements
        file_name_list = file_name.split(".csv")[0].split("_")

        # Extract date and date time from list
        source_file_date_str = file_name_list[-2]
        source_file_date_time_str = file_name_list[-2] + file_name_list[-1]

        logging.info("source_file_date_str= {}".format(source_file_date_str))
        logging.info("source_file_date_time_str= {}".format(source_file_date_time_str))

        # Return list of timestamp by converting string to datetime
        return [pd.to_datetime(source_file_date_str), pd.to_datetime(source_file_date_time_str)]
    except:
        return [None, None]

def flatten_json_columns(df, json_cols, custom_df):
    """
    This function flattens JSON columns to individual columns
    It merges the flattened dataframe with expected dataframe to capture missing columns from JSON
    :param df: HR Data CSV raw dataframe
    :param json_cols: custom data columns in CSV's
    :param custom_df: expected dataframe
    :return: returns df pandas dataframe
    """

    # Loop through all JSON columns
    for column in json_cols:
        if not df[column].isnull().all():
            # create a temp col to preserve the orginal data
            df['custom_data_temp'] = df[column]
            # Replace None and NaN with empty braces
            df[column].fillna(value='{}', inplace=True)
            try:
                # Deserialize's a str instance containing a JSON document to a Python object
                df[column] = [json.loads(row, strict=False) for row in df[column]]
            except TypeError:
                # Convert all values to string using literal eval
                df[column] = df[column].apply(lambda x: literal_eval(str(x)))
            # Normalize semi-structured JSON data into a flat table
            column_as_df = pd.json_normalize(df[column])
            # Extract main column name and attach it to each sub column name
            column_as_df.columns = [f"{column}_{subcolumn}" for subcolumn in column_as_df.columns]
            # Replace empty strings with None
            column_as_df.replace('', np.nan, inplace=True)
            # Replace orginal data with temp data
            df[column] = df['custom_data_temp']
            # Merge extracted result from custom_data field with expected fields
            result_df = pd.merge(column_as_df, custom_df, how='left')
            # Drop the temp column and merge the flattened dataframe with orginal dataframe
            df = df.drop('custom_data_temp', axis=1).merge(result_df, right_index=True, left_index=True)
        else:
            df = pd.concat([df, custom_df], axis=1)

    # Return dataframe with flatten columns
    return df

def parse_hr_data_csv_df(df, bucket_name, bucket_prefix, entity_name):
    """
    Parse HR Data CSV column(s) values from the dataframe
    :param df: HR Data CSV file path on the S3
    :param bucket_name: AWS S3 bucket name
    :param bucket_prefix: AWS S3 bucket prefix
    :param entity_name: HR Data CSV entity name
    :return: returns df pandas dataframe
    """

    # Check if the entity is part of json columns schema
    if entity_name in json_columns_schema.keys():

        # Pre defined custom data expected data frame
        custom_data_expected_df=pd.DataFrame.from_dict(custom_data_schema[entity_name])

        if entity_name == 'user_point_transactions':
            df['reference_type_json'] = [column if column[0] == '{' else None for column in df['reference_type']]
            df['reference_type'] = [None if column[0] == '{' else column for column in df['reference_type']]

            # Flatten reference_type_json CSV column to individual columns
            upt_df = flatten_json_columns(df=df,
                                          json_cols=json_columns_schema[entity_name],
                                          custom_df=custom_data_expected_df)

            parsed_df = upt_df.drop('reference_type_json', axis=1)
        else:
            # Flatten CSV JSON Columns to individual columns
            parsed_df = flatten_json_columns(df=df,
                                             json_cols=json_columns_schema[entity_name],
                                             custom_df=custom_data_expected_df)
    else:
        # Return raw dataframe
        parsed_df = df

    # Get file name based on s3 source path
    hr_data_csv_file_name = get_file_name(ib_bucket_name=bucket_name, ib_bucket_prefix=bucket_prefix)
    logging.info("hr_data_csv_file_name= {}".format(hr_data_csv_file_name))

    # Create file name column in parsed data frame
    parsed_df['source_file_name'] = hr_data_csv_file_name

    # Get source file date and datetime into a list based on HR Data csv file name
    hr_data_csv_source_file_date_and_datetime = get_source_file_date_time(file_name = hr_data_csv_file_name)

    # Create source file date and datetime column(s) in parsed data frame
    parsed_df['source_file_date'] = hr_data_csv_source_file_date_and_datetime[0]
    parsed_df['source_file_datetime'] = hr_data_csv_source_file_date_and_datetime[1]

    # Return pandas dataframe
    return parsed_df

'''
CSV Parsing function with parameters
'''

def parsing_launcher(ib_bucket_name, ib_bucket_name_prefix, dlp_bucket_name, dlp_bucket_name_prefix, hive_style_partitions, source_entities_list):
    """
    Invokes the HR Data CSV Parser to ingest data to data lake
    :param ib_bucket_name: AWS S3 bucket name in data platform accounts
    :param ib_bucket_name_prefix: AWS S3 bucket name prefix
    :param dlp_bucket_name: AWS S3 data lake processing bucket name in data platform accounts
    :param dlp_bucket_name_prefix: AWS S3 datalake processing bucket name prefix
    :param hive_style_partitions: S3 partitions to read the file
    :param source_entities_list: List of entities to process
    :return: None
    """

    for entity in source_entities_list:

        logging.info("entity_name= {}".format(entity))

        # Constructing source path based on source entity
        s3_bucket = "s3://" + ib_bucket_name + "/"
        s3_bucket_prefix = ib_bucket_name_prefix + "/" + entity + "/" + hive_style_partitions
        source_path = s3_bucket + s3_bucket_prefix
        logging.info("Full source_path {}".format(source_path))

        # Constructing target path based on source entity
        target_path =  "s3://" + dlp_bucket_name + "/" + dlp_bucket_name_prefix + "/" + entity + "/" + hive_style_partitions
        logging.info("Full target_path {}".format(target_path))

        # Read HR Data CSV data from S3 path
        raw_df = read_hr_data_csv(file_path=source_path,
                                                file_schema=read_schema[entity],
                                                date_cols=date_columns_schema[entity])

        # If raw data frame is not empty then parse it
        if not raw_df.empty:
            # Parse CSV data
            parsed_hr_data_df = parse_hr_data_csv_df(df=raw_df,
                                                                                 bucket_name=ib_bucket_name,
                                                                                 bucket_prefix=s3_bucket_prefix,
                                                                                 entity_name= entity)

            try:
                # Convert column data types using astype with write schema
                hr_data_astype_dtypes = parsed_hr_data_df.astype(write_schema[entity], copy=True)
            except:
                # Convert column data types using astype with write schema and ignore errors
                hr_data_astype_dtypes = parsed_hr_data_df.astype(write_schema[entity], copy=True, errors='ignore')

            # Write result to S3
            write_to_s3(df=hr_data_astype_dtypes,
                        target_path=target_path)

if __name__ == "__main__":
    # Accept parameters
    s3_bucket_name = sys.argv[1]
    s3_bucket_name_prefix = sys.argv[2]
    s3_dlp_bucket_name = sys.argv[3]
    s3_dlp_bucket_name_prefix = sys.argv[4]
    date_partitions = sys.argv[5]
    list_of_source_entities = literal_eval(sys.argv[6])

    # Invoke launcher
    parsing_launcher(ib_bucket_name=s3_bucket_name,
                     ib_bucket_name_prefix=s3_bucket_name_prefix,
                     dlp_bucket_name=s3_dlp_bucket_name,
                     dlp_bucket_name_prefix=s3_dlp_bucket_name_prefix,
                     hive_style_partitions=date_partitions,
                     source_entities_list=list_of_source_entities)