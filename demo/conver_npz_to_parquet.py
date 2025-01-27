from typing import List

import numpy as np
import pandas as pd


key_list1 = ["id_transaction","date_transaction","prix","departement","id_ville","ville","code_postal","adresse","type_batiment","n_pieces","surface_habitable","latitude","longitude"]

def convert_npz_to_parquet(npz_file_path:str, col_list:List[str], parquet_file_out_path:str):
    """
    This function converts the france immo transaction npz file into parquet file.
    :param parquet_file_out_path:
    :param npz_file_path:
    :param col_list: A required column name list to be converted.
    :return:
    """

    with np.load(npz_file_path) as npz_data:
        # dict to store the converted value of each column
        data = {}
        # loop through each key value pair of the numpy array
        # key is the column name, value is the column value
        for key, value in dict(npz_data).items():
            # select only the required columns
            if key in col_list:
                print(f"treating {key}")
                # converts byte arrays (encoded as np.uint8) into strings by splitting the byte sequence at the null
                # byte (\x00) and decoding each piece into a string. If the data is not byte data, it leaves it unchanged
                if value.dtype == np.uint8:
                    out_val = [s.decode("utf-8") for s in value.tobytes().split(b"\x00")]
                else:
                    out_val = value
                # add the column to the result dict
                data[key] =  out_val
        # check the result data frame columns
        print(f"result data frame colum list: {data.keys()}")
        # convert to pandas dataframe
        resu_pdf = pd.DataFrame.from_dict(data)
        # write as a parquet file
        resu_pdf.to_parquet(parquet_file_out_path, engine="pyarrow")
