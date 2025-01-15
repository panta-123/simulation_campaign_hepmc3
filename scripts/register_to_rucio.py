#!/usr/bin/env python3

import argparse
import os
import glob
import sys
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

# Extract other arguments
parent_directory = os.path.dirname(did_name)
scope = args.scope
rse = args.rse

# Expand glob patterns into actual file paths
file_paths = []
if not os.path.isfile():
   for pattern in args.file_paths:
     expanded_paths = glob.glob(pattern)
     if expanded_paths:
        file_paths.extend(expanded_paths)
     else:
        print(f"Warning: No files matched the pattern '{pattern}'.")
   if not file_paths:
     print("Error: No files found to register.")
     sys.exit(1)
else:
   file_paths.append(args.file_path)


base_did_name = args.did_name
scope = args.scope
rse = args.rse

# Create upload items for each file with dynamic DID names
uploads_items = []
for file_path in file_paths:
    filename = os.path.basename(file_path)
    dynamic_did_name = f"{base_did_name}{filename}"  # Construct DID name
    uploads_items.append({
        'path': file_path,
        'rse': rse,
        'did_scope': scope,
        'did_name': dynamic_did_name,
        'dataset_scope': scope,
        'dataset_name': os.path.dirname(base_did_name)
    })



logger = logging.getLogger('upload_client')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)
upload_client=UploadClient(logger=logger)
upload_client.upload(uploads_items)
