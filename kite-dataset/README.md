kite-dataset
============

Example app for writing partitioned data using Kite SDK

Build:
 mvn clean package

Run:
 java -jar target/kite-dataset-0.1.0.jar

Delete the data in HDFS before running again: 
 hdfs dfs -rm -r /user/${USER}/demo/fileinfo