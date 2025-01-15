#!/usr/bin/env python3

import argparse
import os
import glob
from rucio.client.uploadclient import UploadClient
from rucio.common.exception import InputValidationError, RSEWriteBlocked, NoFilesUploaded, NotAllFilesUploaded
import logging

# Define the argument parser
parser = argparse.ArgumentParser(prog='Register to RUCIO', description='Registers files to RUCIO')
parser.add_argument("-f", dest="file_paths", nargs="store", action="store", required=True, help="Enter the local file paths (glob patterns)")
parser.add_argument("-d", dest="did_name", action="store", required=True, help="Enter the data identifier for rucio catalogue")  
parser.add_argument("-s", dest="scope", action="store", required=True, help="Enter the scope")
parser.add_argument("-rse", dest="rse", action="store", required=True, help="Enter the RSE name")
args = parser.parse_args()

# Expand glob patterns into actual file paths
file_paths = []
for pattern in args.file_paths:
    expanded_paths = glob.glob(pattern)
    if expanded_paths:
        file_paths.extend(expanded_paths)
    else:
        print(f"Warning: No files matched the pattern '{pattern}'.")

# Extract other arguments
did_name = args.did_name
parent_directory = os.path.dirname(did_name)
scope = args.scope
rse = args.rse

# Create upload items for each file
uploads_items = []
for file_path in file_paths:
    uploads_items.append({
        'path': file_path,
        'rse': rse,
        'did_scope': scope,
        'did_name': did_name,
        'dataset_scope': scope,
        'dataset_name': parent_directory
    })



logger = logging.getLogger('upload_client')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)
upload_client=UploadClient(logger=logger)
upload_client.upload(uploads_items)
