#!/usr/bin/env python3

import argparse
import os
from rucio.client.uploadclient import UploadClient
from rucio.common.exception import InputValidationError, RSEWriteBlocked, NoFilesUploaded, NotAllFilesUploaded
import logging

parser = argparse.ArgumentParser(prog='Register to RUCIO', description='Registers files to RUCIO')
parser.add_argument("-f", dest="file_path", action="store", required=True, help="Enter the local file path")
parser.add_argument("-d", dest="did_name", action="store", required=True, help="Enter the data identifier for rucio catalogue")  
parser.add_argument("-s", dest="scope", action="store", required=True, help="Enter the scope")
parser.add_argument("-r", dest="rse", action="store", required=True, help="Enter the rucio storage element. EIC-XRD is for storing production outputs.")

args=parser.parse_args()

file_path = args.file_path
did_name = args.did_name
parent_directory = os.path.dirname(did_name)
scope= args.scope
rse= args.rse   

uploads_items = [{
        'path': file_path,
        'rse': rse,
        'did_scope': scope,
        'did_name': did_name,
        'dataset_scope': scope,
        'dataset_name': parent_directory
}]

logger = logging.getLogger('upload_client')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)
upload_client=UploadClient(logger=logger)
upload_client.upload(uploads_items)
