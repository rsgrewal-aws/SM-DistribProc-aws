
"""Feature engineers the distributed data set using tar ball """
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
import tarfile

import pathlib
import shutil
import traceback


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
    

    for arg, value in sorted(vars(args).items()):
        _logger.info(f"TAR:logging:all:environment:variables={arg}::value={value}::")

    print("printing the env vars now")
    env_var = os.environ    
    import pprint
    pprint.pprint(dict(env_var), width = 1)
    
    # Split list of files into sub-lists
    cpu_count = multiprocessing.cpu_count()
    
    BASE_DIR = "/opt/ml/processing"
    
    print(input_dir)
    _logger.info(f"Input:data:path:={input_dir}::")

    try:
        with open('/opt/ml/input/config/inputdataconfig.json') as f:
            lines = f.readlines()
            print(lines)
    except:
        print("error in reading input data config")
        
    #fn = os.path.join("/opt/ml/processing/input", "combined_tweets.csv")
    
    onlyFiles = [f for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f))]
    _logger.info(f"TAR:Data Downloaded::Now Reading downloaded data.:dir:{input_dir}::And:FILES:ARE::{onlyFiles}")
    
    # -- ITERATE OVER EACH OF THE TAR FILES which we got On this Instance and process them 
    for tarFiles in onlyFiles:
        # -- Read the TAR file, extract to temp proc directory , then pick up the first file to process it 
        fn = os.path.join(input_dir, tarFiles) #onlyFiles[0])
        proc_dir = f"{input_dir}/filesProc"
        pathlib.Path(proc_dir).mkdir(parents=True, exist_ok=True)

        _logger.info(f"TAR:Creating directory:actual Procesing dir={proc_dir}:file_loc={fn}:")

        # -- extract the TAR to the ./filesProc/data which is the temp location.
        with tarfile.open(fn, 'r:*') as s3_tar:
            s3_tar.extractall(proc_dir)

        _logger.info(f"TAR FILE Extracted successfully to dir={proc_dir}")  
        allFiles = [f for f in os.listdir(proc_dir) ]
        _logger.info(f"TAR FILE listing :{proc_dir}: gives us :{allFiles}:") 

        # -- Create the location for pandas to read the EXTRACTED CSV file  - similiar to ./filesProc/data/test_01.csv
        proc_data_dir = os.path.join(proc_dir, "data")
        _logger.info(f"TAR FILE :proc_data_dir:to:{proc_data_dir}::")
        onlyFiles = [f for f in os.listdir(proc_data_dir) if os.path.isfile(os.path.join(proc_data_dir, f))]
        _logger.info(f"TAR: After extraction:READ::dir:{proc_data_dir}::And:FILES:ARE::{onlyFiles}")
        fn = os.path.join(proc_data_dir, onlyFiles[0])
        _logger.info(f"TAR: After extraction:FILE:loc={fn}::")

        # -- Read in csv at ./filesProc/data/test_01.csv
        df = pd.read_csv(fn, names=columns)
        _logger.info(f"DF:SHARD:INDEX={df['SHARD_PREFIX'].to_list()}::")

        # -- DO some processing on this CSV file read 
        _logger.info(f"AFTER:READ:Going to write it to {BASE_DIR}/output : file will be {BASE_DIR}/output/{onlyFiles[0]}:: ")
        _logger.info(f"READ:Data:len={len(df)}::  shape={df.shape}::")
        df.to_csv(
            f"{BASE_DIR}/output/{onlyFiles[0]}", header=True, index=False
        )

        # -- Clean up the temp diectories ./filesProc so we can repeat for the other files
        try:
            shutil.rmtree(proc_dir)
            _logger.info(f"Temp directory={proc_dir}:: deleted")
        except :
            _logger.info(f"Error: in deleting the TEMP dir={proc_dir}::using traceback={traceback.format_exc()}::")

        
    _logger.info("All Done !! written out !!")
    
    # ----------------   OLD TEMPLATE CODE --------------------#
