from pathlib import Path
import pyarrow.parquet as pq

"""
In spark, the default Row Group Size is 128 MB (134217728 bytes):The maximum size of a row group before starting a new one.
Page Size	1 MB (1048576 bytes)	The size of data pages within a row group.
# for spark to write parquet with customize row group size and page size
df.write.option("parquet.block.size", "67108864") \  # Set Row Group Size to 64MB
       .option("parquet.page.size", "524288") \      # Set Page Size to 512KB
       .parquet("output.parquet")
"""

def write_parquet_with_advance_options(in_parquet_path:str, out_parquet_path:str,
                                       max_row_group_size:int=500000, max_page_size:int=65536, compression_algo:str="snappy", ):
    # read the parquet file as arrow table
    arrow_table = pq.read_table(in_parquet_path)

    # write the arrow table as a parquet file
    pq.write_table(arrow_table, out_parquet_path, compression=compression_algo, row_group_size=max_row_group_size,
                   data_page_size=max_page_size)

def main():
    data_path = Path.cwd().parent / "data"
    immo_nano_ts_path = (data_path / "fr_immo_transactions.parquet").as_posix()




if __name__ == "__main__":
    main()