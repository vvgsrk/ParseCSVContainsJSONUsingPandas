### ParseCSVContainsJSONUsingPandas
This parser parses multiple csv files at the same time and also flattens JSON columns in the CSV

### Data format

The file format is CSV and it contains custom_data fields as JSON value in some columns. 

### Data processing

A python script is used to parse the CSV data file's.
CSV files schema created as a python dictionary and fetched based on entity name when reading the file.
All Date fields of all csv files are listed and parsed with parse_dates option of pandas library.
Custom data fields contains JSON data hence a utility function flattens it. 

Finally, To write parquet data to S3 data lake processing bucket, it uses `awswrangler`
[AWS Data Wrangler](https://github.com/awslabs/aws-data-wrangler "AWS Data Wrangler")