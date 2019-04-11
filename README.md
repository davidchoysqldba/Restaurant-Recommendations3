# Restaurant-Recommendations3

1.
The download_to_google_cloud.py does following:
Backfills data from 12/2014
Fetches the json format of the file and loads into GCP

2.
EDA_RestaurantFlow_Google_Cloud is the Jupyter notebook in GCP contains core ELT logic:
Spark
Python

3.
Work is under way to implement and deploy 2. into GCP production:
Package build - pip
Creation and destroy of Dataproc image (Spark Cluster image)
Scheduling of daily pyspark job via Airflow in GCP
