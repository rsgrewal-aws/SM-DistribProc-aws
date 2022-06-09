
"""Feature engineers the distributed data set """
import argparse
import logging
import os
import pathlib
import requests
import tempfile

import boto3
import numpy as np
import pandas as pd

import json

import multiprocessing

_logger = logging.getLogger()
_logger.setLevel(logging.INFO)
_logger.addHandler(logging.StreamHandler())

try:
    _logger.info(f"Pandas:version:{pd.__version__}")
    _logger.info(f"Numpy:version:{np.__version__}")
except:
    pass


# Since we get a headerless CSV file we specify the column names here.

# Since we get a headerless CSV file we specify the column names here.

columns=[
    "SHARD_PREFIX",
    "YEAR_BUILT",
    "SQUARE_FEET",
    "NUM_BEDROOMS",
    "NUM_BATHROOMS",
    "LOT_ACRES",
    "GARAGE_SPACES",
    "FRONT_PORCH",
    "DECK",
    "PRICE"
]


columns_dtype = {
    'SHARD_PREFIX': np.int, 
    "YEAR_BUILT": np.int,
    "SQUARE_FEET": np.float64,
    "NUM_BEDROOMS": np.int,
    "NUM_BATHROOMS": np.int,
    "LOT_ACRES": np.float64,
    "GARAGE_SPACES": np.int,
    "FRONT_PORCH": np.int,
    "DECK": np.int,
    "PRICE": np.float64
    #   'body': str, 
}

def merge_two_dicts(x, y):
    """Merges two dicts, returning a new copy."""
    z = x.copy()
    z.update(y)
    return z

def listLocalDirectory(dirPath="."):
    for path, dnames, fnames in os.walk(dirPath):
        _logger.info(f"List::path={path}::dirNames={dnames}::fileNames={fnames}::")

    print("list:done:")
       
if __name__ == "__main__":
    _logger.info("Starting distprocess.py.")
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-size", type=int, default=100)
    parser.add_argument("--input-path", type=str, default='/opt/ml/processing/input') # "input_path", "/opt/ml/processing/input",
    args = parser.parse_args()
    data_size = args.data_size
    input_dir = args.input_path

    # Split list of files into sub-lists
    cpu_count = multiprocessing.cpu_count()

    
    BASE_DIR = "/opt/ml/processing"
    
    print(input_dir)
    _logger.info(f"Input:data:path:={input_dir}::")


    #fn = os.path.join("/opt/ml/processing/input", "combined_tweets.csv")
    
    onlyFiles = [f for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f))]
    _logger.info(f"Data Downloaded::Now Reading downloaded data.:dir:{input_dir}::And:FILES:ARE::{onlyFiles}")
    
    fn = os.path.join(input_dir, onlyFiles[0])
    
    # read in csv
    df = pd.read_csv(fn, names=columns)
    #_logger.info(f"df:head={df.head()}")
    _logger.info(f"DF:SHARD:INDEX={df['SHARD_PREFIX'].to_list()}::")
    
    _logger.info(f"AFTER:READ:Going to write it to {BASE_DIR}/output : file will be {BASE_DIR}/output/{onlyFiles[0]}:: ")
    _logger.info(f"READ:Data:len={len(df)}::  shape={df.shape}::")
    df.to_csv(
        f"{BASE_DIR}/output/{onlyFiles[0]}", header=True, index=False
    )
    
    _logger.info("All Done !! written out !!")
    
    # ----------------   OLD TEMPLATE CODE --------------------#
