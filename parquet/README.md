# Parquet file format



## The internal structure of a Parquet file

A dataset in parquet format may contain one(without partition) or multiple parquet files(with partition). In one single 
parquet file, it has a hierarchical structure to store data and metadata to reduce the file scan size during data analytics.

The below figure shows the general architecture of a single parquet file.

![parquet_format_overview](../images/parquet_format.gif)

You can notice the below components:
1. Header: a four-byte magic number which indicates this is a parquet file.
2. Row groups:
3. Footer: Stores metadata of the file
4. a four-byte magic number: indicates the end of parquet file. ensure integrity. 

>  No matter what data a Parquet file contains, its header always starts with "PAR1" and ends with "PAR1".
##

