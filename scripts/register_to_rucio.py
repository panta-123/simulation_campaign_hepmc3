#!/usr/bin/env python3

import argparse
import os
from rucio.client.uploadclient import UploadClient
from rucio.client import Client
from rucio.common.exception import DataIdentifierAlreadyExists, InputValidationError, NoFilesUploaded, NotAllFilesUploaded.
import logging

parser = argparse.ArgumentParser(prog='Register to RUCIO', description='Registers files to RUCIO')
parser.add_argument("-f", dest="file_paths", action="store", nargs='+', required=True, help="Enter the local file path")
parser.add_argument("-d", dest="did_names", action="store", nargs='+', required=True, help="Enter the data identifier for rucio catalogue")  
parser.add_argument("-s", dest="scope", action="store", required=True, help="Enter the scope")
parser.add_argument("-r", dest="rse", action="store", required=True, help="Enter the rucio storage element. EIC-XRD is for storing production outputs.")
parser.add_argument('-noregister', dest="noregister", action="store_true", default=False, help="Specify if rucio registration should be skipped")

args=parser.parse_args()

file_paths = args.file_paths
did_names = args.did_names
scope= args.scope
rse= args.rse
noregister = args.noregister 

# Validation to ensure file_paths and did_names have the same length
if len(file_paths) != len(did_names):
    raise ValueError("The number of file paths must match the number of did names.")

# ----------------------------
# Prepare upload items
# ----------------------------

upload_items = []  # List to hold the upload items

# Loop through the file paths and did names (assuming did_names length matches file_paths length)
for file_path, did_name in zip(file_paths, did_names):
    parent_directory = os.path.dirname(did_name)  # Get the parent directory from did_name
        # Create a new dictionary for each file and did_name
        upload_item = {
            'path': file_path,
            'rse': rse,
            'did_scope': scope,
            'did_name': did_name,
            'dataset_scope': scope,
            'dataset_name': parent_directory,
            'no_register': noregister
        }

    # Append the new item to the upload_items list
    upload_items.append(upload_item)


# ----------------------------
# Logger Setup
# ----------------------------
logger = logging.getLogger('upload_client')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

# ----------------------------
# Upload Logic
# ----------------------------
upload_client = UploadClient(logger=logger)
rc_client = RucioClient()

try:
    upload_client.upload(upload_items)

except (NoFilesUploaded, NotAllFilesUploaded) as e:
    logger.warning(f"Upload failed or incomplete: {e}")

    for item in upload_items:
        scope = item['did_scope']
        name = item['did_name']

        try:
            replicas = list(rc_client.list_replicas([{'scope': scope, 'name': name}], rse_expression=rse, all_states=True))
            if replicas:
                states = replicas[0].get('states', {})
                state = states.get(rse)

                if state == 'C':  # C = COPYING
                    logger.info(f"Replica {name} in state COPYING. Setting tombstone to force re-copy in next iteration.")
                    rc_client.set_tombstone(scope=scope, name=name, rse=rse)
        except Exception as replica_error:
            logger.error(f"Error while checking or modifying replica for {name}: {replica_error}")

    raise  # re-raise the original upload error