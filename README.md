# Restaurant-Recommendations3

1.
The download_to_google_cloud.py does following:
Backfills data from 12/2014
Fetches the json format of the file and loads into GCP

2. 
etl_in_google_cloud.py calls the Load_Star_Schema_Restaurant.py (3. file) in the cloud 

3.
Load_Star_Schema_Restaurant.py is the pyspark job that parses the download json files and makes a set of files for Big Query to read for analysis; This file resides at the specified Google Cloud Storage area.
This job will be refactored later into OOP style.
spark-submit Load_Star_Schema_Restaurant.py <path-of-data> <path-of-bigquery-dataset>
