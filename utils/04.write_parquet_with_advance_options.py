from pathlib import Path
import pyarrow.parquet as pq
import json

"""
In spark, the default Row Group Size is 128 MB (134217728 bytes):The maximum size of a row group before starting a new one.
Page Size	1 MB (1048576 bytes)	The size of data pages within a row group.
# for spark to write parquet with customize row group size and page size
df.write.option("parquet.block.size", "67108864")   # Set Row Group Size to 64MB
       .option("parquet.page.size", "524288")      # Set Page Size to 512KB
       .parquet("output.parquet")
"""

def write_parquet_with_advance_options(in_parquet_path:str, out_parquet_path:str,
                                       max_row_group_size:int=500000, max_page_size:int=65536,
                                       compression_algo:str="snappy", table_metadata:dict = None,
                                       column_metadata:dict = None, encoding:str="utf-8",):
    """
    This function takes a parquet file path, rewrite a new parquet file with the given advance options by using pyarrow.
    Pyarrow does not have a default row group max size. It auto determines based on the machine hardware.

    :param in_parquet_path:
    :param out_parquet_path:
    :param max_row_group_size:
    :param max_page_size:
    :param compression_algo:
    :param table_metadata:
    :param column_metadata:
    :param encoding:
    :return:
    """
    # read the parquet file as arrow table
    arrow_table = pq.read_table(in_parquet_path)

    # if custom metadata exists, add them into the parquet file
    if table_metadata:
        table_metadata_bytes = {key: value.encode(encoding) for key, value in table_metadata.items()}
    else:
        table_metadata_bytes = {}
    if column_metadata:
        column_metadata_bytes =  {col: json.dumps(desc).encode(encoding) for col, desc in column_metadata.items()}
    else:
        column_metadata_bytes = {}

    # add metadata to the parquet file
    arrow_table = arrow_table.replace_schema_metadata({**table_metadata_bytes, **column_metadata_bytes})

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
    table_metadata = {
        "owner": "Pengfei",
        "organization": "CASD",
        "data_version": "1.0.0",
    }

    column_metadata = {
        "row.metadata": {"type": "struct", "fields": [
            {"name": "id_transaction", "type": "integer",  "metadata": {"description": "Id de transaction (add by pengfei)"}},
            {"name": "date_transaction", "type": "timestamp",  "metadata": {"description": "Date de transaction (add by pengfei)"}},
            {"name": "prix", "type": "double",  "metadata": {"description": "Le prix de bien en euros (add by pengfei)"}},
            {"name": "departement", "type": "string",  "metadata": {"description": "bla bla (add by pengfei)"}},
            {"name": "id_ville", "type": "integer",  "metadata": {"description": "bla bla (add by pengfei)"}},
            {"name": "ville", "type": "string",  "metadata": {"description": "bla bla (add by pengfei)"}},
            {"name": "code_postal", "type": "integer",  "metadata": {"description": "bla bla (add by pengfei)"}},
            {"name": "adresse", "type": "string",  "metadata": {"description": "bla bla (add by pengfei)"}},
            {"name": "type_batiment", "type": "string",  "metadata": {"description": "bla bla (add by pengfei)"}},
            {"name": "n_pieces", "type": "integer",  "metadata": {"description": "bla bla (add by pengfei)"}},
            {"name": "surface_habitable", "type": "integer",  "metadata": {"description": "bla bla (add by pengfei)"}},
            {"name": "latitude", "type": "double",  "metadata": {"description": "bla bla (add by pengfei)"}},
            {"name": "longitude", "type": "double",  "metadata": {"description": "bla bla (add by pengfei)"}}]}

    }

    write_parquet_with_advance_options(immo_valid_path,
                                       immo_large_row_group,
                                       max_row_group_size=999999999,
                                       max_page_size=max_page_size_1mb,
                                       table_metadata=table_metadata,
                                       column_metadata=column_metadata,)




if __name__ == "__main__":
    main()