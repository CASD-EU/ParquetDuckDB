# ParquetDuckDB

In this repo, we try to understand the format parquet. Realize a little benchmark between CSV and parquet.


## Data source

The data which we will use in this repo is from [kaggle](https://www.kaggle.com/datasets/benoitfavier/immobilier-france). It contains several interesting datasets. In this repo,
we use two of them: 
- foyers_fiscaux.csv: It contains average revenue per household per city between 2014 and 2022
- transaction.npz: It contains all French real state transactions between 2012 and 2024

We can't store the data in our github repo, so you need to download the data from kaggle by yourself, and adapt the name
for the notebooks to work properly.


### Convert npz file to parquet

We provide a script `ParquetDuckDB/utils/01.conver_npz_to_parquet.py` to convert the .npz file to .parquet.

You need to adapt the source file path and output file path to your local setup.


### Convert the parquet file to another file format

We also provide a script `ParquetDuckDB/utils/03.convert_parquet_to_other_format.py` to allow user to partition the parquet file
or convert it to .CSV.



