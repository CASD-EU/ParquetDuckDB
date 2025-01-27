"""
This script converts raw parquet files of fr_immo_transactions.parquet to csv files.
It writes the date column as string 2013-12-31 10:25:51.804819
"""
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


def clean_raw_parquet(spark:SparkSession,immo_raw_parquet_path)->DataFrame :
    """
    This function converts ts column of the long type in raw parquet files to datetime column.
    :param spark: a valid spark session
    :param immo_raw_parquet_path: raw parquet file path
    :return: dataframe with correct datetime column
    """
    # origin date col name
    origin_ts_col_name = "date_transaction"
    tmp_ts_col_name = f"{origin_ts_col_name}_ts"

    # read the raw parquet file
    fr_immo_df = spark.read.parquet(immo_raw_parquet_path)
    # convert a long column type to a datetime column type
    fr_immo_valid_date_df = (fr_immo_df.withColumn(tmp_ts_col_name,
                                                  (col(origin_ts_col_name) / 1_000_100_000).cast("timestamp"))
                             .drop(origin_ts_col_name)).withColumnRenamed(tmp_ts_col_name, origin_ts_col_name)
    return fr_immo_valid_date_df


def convert_raw_parquet_to_valid_parquet(spark:SparkSession,immo_raw_parquet_path, immo_valid_parquet_path):
    """
    This function calls the clean_raw_parquet function to get a valid dataframe and write it in a
     given parquet file path.
    :param spark: a valid spark session
    :param immo_raw_parquet_path: the file path of the raw parquet file
    :param immo_valid_parquet_path: the file path of the valid parquet file
    :return:
    """
    # column with order
    columns = ["id_transaction", "date_transaction", "prix", "departement", "id_ville", "ville", "code_postal",
               "adresse", "type_batiment", "n_pieces", "surface_habitable", "latitude", "longitude"]
    # origin date col name
    fr_immo_valid_df = clean_raw_parquet(spark,immo_raw_parquet_path)
    fr_immo_valid_df.select(columns).coalesce(1).write.mode("overwrite").parquet(immo_valid_parquet_path)

def convert_parquet_to_csv(spark:SparkSession,parquet_file_path:str, csv_file_path:str):
    """
    This function read a parquet file as dataframe and convert it to csv file.
    :param spark: a valid spark session
    :param parquet_file_path:
    :param csv_file_path:
    :return:
    """
    # origin date col name
    df = spark.read.parquet(parquet_file_path)
    columns = ["id_transaction","date_transaction","prix","departement","id_ville","ville","code_postal","adresse","type_batiment","n_pieces","surface_habitable","latitude","longitude"]
    df.select(columns).coalesce(1).write.option("header",
                                True).mode("overwrite").csv(csv_file_path)

def main():
    spark = SparkSession.builder.master("local[4]") \
        .appName("ReadWriteParquet") \
        .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
        .getOrCreate()
    data_path = Path.cwd().parent / "data"
    immo_nano_ts_path = (data_path / "fr_immo_transactions.parquet").as_posix()
    immo_micro_ts_path = (data_path / "fr_immo_transactions_clean").as_posix()
    immo_valid_parquet_path = (data_path / "fr_immo_transactions_valid_ts.parquet").as_posix()
    immo_csv_path = (data_path / "fr_immo_transactions_clean_csv").as_posix()

    convert_raw_parquet_to_valid_parquet(spark,immo_nano_ts_path, immo_micro_ts_path)
    # convert_parquet_to_csv(spark, immo_valid_parquet_path, immo_csv_path)




if __name__ == "__main__":
    main()