#!/usr/bin/env python3

import argparse
import os
import json
import logging
from typing import Dict, Any
from rucio.client.uploadclient import UploadClient
from rucio.client import Client
from rucio.common.exception import InputValidationError, RSEWriteBlocked, NoFilesUploaded, NotAllFilesUploaded

# Define the metadata schema
METADATA_SCHEMA = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "ePICRucioMetadataTags",
  "description": "Optimized metadata tags for ePIC Rucio datasets using searchable slugs.",
  "type": "object",
  "properties": {
    "container_tag": {
      "type": "string",
      "description": "Container version tag (e.g. v25.06.2)",
      "pattern": "^v[0-9]+\.[0-9]+\.[0-9].*$"
    },
    "physics_process": {
      "type": "array",
      "description": "One or more PWG processes. Use slugs for easy database querying.",
      "items": {
        "type": "string",
        "enum": [
          "exclusive_diff_tag",
          "inclusive",
          "jets_hf",
          "semi_inclusive",
          "ew_bsm",
          "other"
        ]
      },
      "minItems": 1,
      "uniqueItems": true
    },
    "min_max_q2": {
      "type": "object",
      "description": "Q2 range (GeV^2). Optional - not applicable to all datasets.",
      "properties": {
        "min": { "type": "number" },
        "max": { "type": "number" }
      },
      "required": ["min", "max"]
    },
    "e_energy": {
      "type": "number",
      "description": "Electron beam energy (GeV)"
    },
    "a_energy": {
      "type": "number",
      "description": "Ion/nucleus beam energy (GeV)"
    },
    "background": {
      "type": "string",
      "description": "Background configuration or setting"
    },
    "a_species": {
      "type": "string",
      "description": "Ion/nucleus species. Defaults to 'p'",
      "default": "p"
    },
    "generator": {
      "type": "string",
      "description": "Generator name (e.g., Pythia8, Herwig)"
    }
  },
  "required": [
    "container_tag",
    "physics_process",
    "e_energy",
    "a_energy",
    "background",
    "a_species",
    "generator"
  ]
} 



def validate_metadata(metadata: Dict[str, Any]) -> bool:
    """
    Validate metadata against the schema.
    
    Parameters
    ----------
    metadata : dict
        The metadata dictionary to validate
        
    Returns
    -------
    bool
        True if valid
        
    Raises
    ------
    ValueError
        If metadata doesn't match the schema
    """
    if not isinstance(metadata, dict):
        raise ValueError("Metadata must be a JSON object (dictionary)")
    
    # Check that values match expected types if they're present
    for key, value in metadata.items():
        if key in METADATA_SCHEMA["properties"]:
            expected_types = METADATA_SCHEMA["properties"][key]["type"]
            if isinstance(expected_types, list):
                type_map = {"string": str, "integer": int}
                valid_types = tuple(type_map[t] for t in expected_types if t in type_map)
                if not isinstance(value, valid_types):
                    raise ValueError(
                        f"Metadata field '{key}' has invalid type. "
                        f"Expected one of {expected_types}, got {type(value).__name__}"
                    )
            else:
                type_map = {"string": str, "integer": int, "object": dict}
                if expected_types in type_map and not isinstance(value, type_map[expected_types]):
                    raise ValueError(
                        f"Metadata field '{key}' has invalid type. "
                        f"Expected {expected_types}, got {type(value).__name__}"
                    )
    
    return True


def load_metadata_file(filepath: str) -> Dict[str, Any]:
    """
    Load and validate metadata from a JSON file.
    
    Parameters
    ----------
    filepath : str
        Path to the metadata JSON file
        
    Returns
    -------
    dict
        The validated metadata dictionary
        
    Raises
    ------
    FileNotFoundError
        If the metadata file doesn't exist
    ValueError
        If the JSON is invalid or doesn't match the schema
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Metadata file not found: {filepath}")
    
    try:
        with open(filepath, 'r') as f:
            metadata = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in metadata file: {e}")
    
    validate_metadata(metadata)
    return metadata



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='Register to RUCIO',
        description='Registers files to RUCIO with optional dataset metadata'
    )
    parser.add_argument(
        "-f", dest="file_paths",
        action="store", nargs='+', required=True,
        help="Enter the local file path(s)"
    )
    parser.add_argument(
        "-d", dest="did_names",
        action="store", nargs='+', required=True,
        help="Enter the data identifier(s) for rucio catalogue"
    )
    parser.add_argument(
        "-s", dest="scope",
        action="store", required=True,
        help="Enter the scope"
    )
    parser.add_argument(
        "-r", dest="rse",
        action="store", required=True,
        help="Enter the rucio storage element (e.g., EIC-XRD for production outputs)"
    )
    parser.add_argument(
        '--noregister', dest="noregister",
        action="store_true", default=False,
        help="Skip rucio registration (upload only)"
    )
    parser.add_argument(
        '--upload-metadata', dest="metadata_file",
        action="store", default=None,
        help="Path to JSON file containing dataset metadata"
    )

    args = parser.parse_args()

    file_paths = args.file_paths
    did_names = args.did_names
    scope = args.scope
    rse = args.rse
    noregister = args.noregister

    # Validation to ensure file_paths and did_names have the same length
    if len(file_paths) != len(did_names):
        raise ValueError("The number of file paths must match the number of did names.")

    # Load and validate metadata if provided
    dataset_meta = None
    if args.metadata_file:
        dataset_meta = load_metadata_file(args.metadata_file)
        print(f"Loaded metadata: {json.dumps(dataset_meta, indent=2)}")

    upload_items = []  # List to hold the upload items

    # Loop through the file paths and did names
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
        
        # Add metadata if provided and not in noregister mode
        if dataset_meta and not noregister:
            upload_item['dataset_meta'] = dataset_meta
        
        # Append the new item to the upload_items list
        upload_items.append(upload_item)

    # Set up logging
    logger = logging.getLogger('upload_client')
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)
    
    upload_client = UploadClient(logger=logger)
    client = Client()
    
    try:
        upload_client.upload(upload_items)
        logger.INFO("Upload completed successfully!")
    except (NoFilesUploaded, NotAllFilesUploaded) as e:
        logger.error(f"Upload failed: {e}")
        
        dids = [{'scope': scope, 'name': did_name} for did_name in did_names]
        
        # Get replicas for all DIDs in the rse
        replicas = client.list_replicas(
            dids,
            all_states=True,
            rse_expression=rse
        )
        
        # Collect files that need to be cleaned up
        files_to_update = []
        files_to_tombstone = []
        
        for replica in replicas:
            did_name = replica['name']
            state = replica['states'].get(rse)
            
            if state == 'COPYING':
                logger.warning(
                    "Found COPYING replica %s:%s on %s — deleting",
                    scope, did_name, rse
                )
                files_to_update.append({'scope': scope, 'name': did_name, 'state': 'U'})
                files_to_tombstone.append({'rse': rse, 'scope': scope, 'name': did_name})
        
        if files_to_update:
            # Update replica states to UNAVAILABLE(U)
            client.update_replicas_states(rse=rse, files=files_to_update)
            # set tombstone to that did, should trigger deletion
            client.set_tombstone(files_to_tombstone)
        
        raise
