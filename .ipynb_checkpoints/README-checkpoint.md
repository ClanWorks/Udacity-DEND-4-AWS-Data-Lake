# Udacity: Data Lake

## Introduction

The aim of this project is to aid an inhouse analytics team by creating an etl pipeline to extract data from Amazon S3, before processing with Spark and loading the processsed data back to Amazon S3.

## How to Run

1. Establish an Amazon EMR cluster.
2. Run **etl.py**: From song_data and log_data, read, transform and load files/data.

**Notes:**  
The dl.cfg file should be configured with your cluster access details.

## File Introduction
### Datasets

#### Song data  
A subset of the million song dataset.  
JSON files containing song and artist metadata.  
First 3 letters of track ID is used for partitioning the data
#### Log Data
JSON files contianing simulated user activity data.  
year and month used to partition data.

### Project Files
**dl.cfg**  
A configuration file containing the data to establish a connection to Amazon EMR  
**etl.py**  
Establishes an EMR connection then reads and processes data from song_data and log_data into tables. 
Parquet tables are then read to an S3 bucket.  
**README.md**  
This interesting discussion.  

## Schema and ETL
![ER Diagram for Sparkify Project](er.png)

The ER diagram corresponding to the Star Schema used in the current project is show above.  
The songplays fact table acts as a convenient and logical node between the given dimension tables.  
The dimension tables are well defined and separated allowing for a high degree of flexibility.  

The raw data is loaded from the S3 bucket and extracted to the tables that constitute the star schema above.
Songplay records are extracted and completed with the aid of the song and log data.
Parquet tables are individually loaded to an S3 bucket.



