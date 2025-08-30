**Spotify Data Engineering Pipeline (Capstone Project)**

This project builds a complete ETL pipeline for Spotify data using Python, AWS, and Airflow.

 **Pipeline Overview**

Extract: Fetch artists, albums, and tracks from the Spotify API with Python.

Transform: Clean and aggregate data using Pandas.

Load: Store raw and processed files in Amazon S3 and load into Amazon Redshift.

Schedule: Automate with Apache Airflow (daily at 10 PM).

Analyze: Run SQL queries in Redshift to generate insights.

 **Tech Stack**

Python (Spotipy, Pandas, Boto3)

AWS (S3, Redshift)

Apache Airflow

SQL

 **Deliverables**

Data lake in S3 (raw + transformed CSVs)

Data warehouse in Redshift (artists, albums, tracks, etc.)

Automated DAG in Airflow

SQL analytics (top artists, albums, track durations, explicit ratio)
