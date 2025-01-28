from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

def convert_nano_ts_to_micro(input_file_path:str, output_file_path:str):
    """
    This function takes a parquet file, find all datetime columns in type datetime64[ns](timestamp in nano seconds),
    can convert them into type datetime64[us](timestamp in microseconds)
    :param input_file_path:
    :param output_file_path:
    :return:
    """
    df = pd.read_parquet(input_file_path, engine="pyarrow")
    # convert the nano second timestamp to micro sceond
    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[col] = df[col].astype('datetime64[us]')
    # Save with compatible timestamp format
    table = pa.Table.from_pandas(df)

    pq.write_table(
        table,
        output_file_path,
        coerce_timestamps="us"  # Forces timestamps to microseconds
    )

def main():
    data_path = Path.cwd().parent / "data"
    immo_nano_ts_path = (data_path / "fr_immo_transactions.parquet").as_posix()
    immo_micro_ts_path = (data_path / "fr_immo_micro_ts_transactions.parquet").as_posix()
    convert_nano_ts_to_micro(immo_nano_ts_path, immo_micro_ts_path)




if __name__ == "__main__":
    main()