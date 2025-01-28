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
                                       max_row_group_size:int=500000, max_page_size:int=65536,
                                       compression_algo:str="snappy", custom_metadata:dict = None ):
    # read the parquet file as arrow table
    arrow_table = pq.read_table(in_parquet_path)

    # if custom metadata exists, add them into the parquet file
    if custom_metadata:
        new_metadata = {key: value.encode() for key, value in custom_metadata.items()}
        arrow_table = arrow_table.replace_schema_metadata(new_metadata)

    # write the arrow table as a parquet file with advance options
    pq.write_table(arrow_table, out_parquet_path, compression=compression_algo, row_group_size=max_row_group_size,
                   data_page_size=max_page_size)




def main():
    data_path = Path.cwd().parent / "data"
    immo_valid_path = (data_path / "fr_immo_transactions_valid_ts.parquet").as_posix()
    immo_large_row_group = (data_path / "fr_immo_transactions_custom_metadata.parquet").as_posix()
    # 512Kb
    max_page_size_512k = 524288
    # 1mb
    max_page_size_1mb = 1048576

    # custom metadata
    custom_metadata = {
        "owner": "Pengfei",
        "organization": "CASD",
        "data_version": "1.0.0",
    }

    write_parquet_with_advance_options(immo_valid_path,
                                       immo_large_row_group,
                                       max_row_group_size=999999999,
                                       max_page_size=max_page_size_1mb,
                                       compression_algo="zstd",
                                       custom_metadata=custom_metadata)




if __name__ == "__main__":
    main()