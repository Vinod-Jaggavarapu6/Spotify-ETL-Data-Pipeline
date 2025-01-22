# Spotify Top 100 Albums ETL Pipeline
This project implements an automated ETL (Extract, Transform, Load) pipeline for Spotify's Top 100 Albums data. The pipeline fetches data from the Spotify API, processes it, and makes it available for analysis using AWS services.

### Architecture

1. **Data Extraction**: AWS Lambda function fetches Top 100 Albums data from Spotify API
2. **Raw Data Storage**: Raw Data stored in S3 bucket
3. **Data Transformation**: Lambda function transforms raw data into separate datasets
4. **Transformed Data Storage**: Processed data stored in S3 in respective folders
5. **Data Cataloging and Querying**: AWS Glue and Amazon Athena used for data management and analysis

### Workflow

1. **Data Extraction**
    - Lambda function triggered weekly by CloudWatch
    - Fetches Top 100 Albums data from Spotify API
    - Stores raw data in S3 bucket: /raw_data/to_processed/
2. **Data Transformation**
    - Second Lambda function triggered by S3 event
    - Processes raw data to extract albums, songs, and artists information
    - Stores transformed data in S3 bucket:
        - /transformed/artist_data/
        - /transformed/albums_data/
        - /transformed/songs_data/
     - Copies processed raw data to /raw_data/processed/
     - Deletes raw data from /raw_data/to_processed/
       
3. **Data Cataloging and Analysis**
    - AWS Glue Crawler scans transformed data
    - Creates tables in AWS Glue Data Catalog
    - Amazon Athena used for querying and analyzing data

  
