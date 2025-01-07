#!/usr/bin/env python3

import argparse
import os
from rucio.client.uploadclient import UploadClient

parser = argparse.ArgumentParser(prog='Register to RUCIO', description='Registers files to RUCIO')
parser.add_argument("-f", dest="file_path", action="store", required=True, help="Enter the local file path")
parser.add_argument("-d", dest="did_name", action="store", required=True, help="Enter the data identifier for rucio catalogue")  
parser.add_argument("-s", dest="scope", action="store", required=True, help="Enter the scope")
args=parser.parse_args()

file_path = args.file_path
did_name = args.did_name
parent_directory = os.path.dirname(did_name)
scope= args.scope
rse="EIC-XRD"  

uploads_items = [{
        'path': file_path,
        'rse': rse,
        'did_scope': scope,
        'did_name': did_name,
        'dataset_scope': scope,
        'dataset_name': parent_directory
}]

upload_client = UploadClient()
upload_client.upload(uploads_items)
