#!/usr/bin/env python3
"""
Register files to Rucio, PanDA-WMS style:

    upload with no_register=True  ->  manual add_replicas  ->  attach + rule

Verified against the PanDA pilot3 rucio copytool (uploads each file with
no_register=True and retries; registration is decoupled) and rucio 41.2.0.

Design notes
------------
* The rucio UploadClient does NOT fail over across RSEs and only "retries" over
  the protocol schemes of the ONE random RSE it picks from an expression
  (uploadclient.py::_pick_random_rse). So per-RSE retry + cross-RSE failover are
  added here.
* With no_register=False it does add_dataset(rules=[grouping='DATASET']) + a
  one-file-at-a-time attach with no ignore_duplicate. When many jobs finish
  together against the same dataset row that deadlocks (DatabaseException, which
  it does NOT catch) and re-runs throw DuplicateContent. So we upload with
  no_register=True and do the catalog side ourselves, idempotently and with
  backoff.
* We do NOT pre-register a COPYING replica + tombstone-on-failure (that leans on
  the reaper daemon and is slow). Instead, like PanDA, if the manual add_replicas
  fails we DELETE THE PHYSICAL FILE immediately via rsemanager.delete(). No dark
  data, no waiting on a daemon.

Dark data vs. orphaned replica
------------------------------
    upload ok, add_replicas FAILED   -> file on disk, nothing in catalog = DARK
                                     -> delete the physical file now.
    add_replicas ok, attach/rule FAILED
                                     -> replica IS registered (catalog knows) =
                                        NOT dark. It is an ORPHANED replica
                                        (registered, not attached). This script
                                        leaves it in place and logs it; orphan
                                        reconciliation is handled by a SEPARATE
                                        process outside this script.

grouping vs. placement
----------------------
grouping is a RULE property (add_replication_rule): DATASET/ALL keep a dataset's
files together on one RSE; NONE spreads them. If you physically --spread files
but make a grouping=DATASET rule, the daemon consolidates them onto one RSE ->
many transfers. grouping defaults from --distribute (DATASET for pack, NONE for
spread); an inconsistent explicit combination is refused.
"""

import argparse
import json
import logging
import os
import random
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from jsonschema import validate as json_validate, ValidationError

from rucio.client import Client
from rucio.client.uploadclient import UploadClient
from rucio.rse import rsemanager as rsemgr
from rucio.common.checksum import adler32, md5
from rucio.common.exception import (
    DatabaseException,
    DataIdentifierAlreadyExists,
    DataIdentifierNotFound,
    DuplicateContent,
    DuplicateRule,
    FileAlreadyExists,
    FileReplicaAlreadyExists,
    InvalidRSEExpression,
    NoFilesUploaded,
    NotAllFilesUploaded,
    RSEWriteBlocked,
    RucioException,
    SourceNotFound,
)

# --------------------------------------------------------------------------- #
# Metadata schema (unchanged)
# --------------------------------------------------------------------------- #
METADATA_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "ePICRucioMetadataTags",
    "description": "Optimized metadata tags for ePIC Rucio datasets using searchable slugs.",
    "type": "object",
    "properties": {
        "software_release": {
            "type": "string",
            "description": "Container version tag (e.g. 26.03.0-stable, nightly, unstable, or default)",
            "pattern": "^([0-9]+\\.[0-9]+\\.[0-9]+-stable|nightly|unstable|default)$"
        },
        "requester_pwg": {
            "type": "string",
            "description": "PWG requesting the dataset.",
            "enum": [
                "edt",
                "inclusive",
                "jets_hf",
                "semi_inclusive",
                "ew_bsm",
                "other"
            ]
        },
        "q2_min_gev2": {
            "type": "number",
            "description": "Minimum Q2 value (GeV^2). Optional - not applicable to all datasets."
        },
        "q2_max_gev2": {
            "type": "number",
            "description": "Maximum Q2 value (GeV^2). Optional - not applicable to all datasets."
        },
        "electron_beam_energy_gev": {
            "type": "number",
            "description": "Electron beam energy (GeV)"
        },
        "ion_beam_energy_gev": {
            "type": "number",
            "description": "Ion/nucleus beam energy (GeV)"
        },
        "is_background_mixed": {
            "type": "boolean",
            "description": "True if the sample includes background mixing; false if it is a regular/pure signal sample."
        },
        "ion_species": {
            "type": "string",
            "description": "Ion species.",
            "enum": [
                "p",
                "Au197",
                "Cu63",
                "He3",
                "H2",
                "Ru96",
                "Pb208",
                "Pb207"                
            ]
        },
        "data_level": {
            "type": "string",
            "description": "Data processing level.",
            "enum": [
                "simulation",
                "reconstruction"
            ]
        },
        "gun_particle": {
            "type": "string",
            "description": "Single particle type. Optional - only applicable to single particle datasets.",
            "enum": [
                "e-",
                "e+",
                "proton",
                "neutron",
                "pi+",
                "pi-",
                "pi0",
                "kaon-",
                "kaon+",
                "gamma",
                "mu-"
            ]
        },
        "geometry_config": {
            "type": "string",
            "description": "Geometry configuration tag (e.g. craterlake_18x275, craterlake_5x41_He3)",
            "pattern": "^[a-z][a-z0-9_]*_[0-9]+x[0-9]+(_.+)?$"
        },
        "gun_momentum_min_gev": {
            "type": "number",
            "description": "Minimum particle gun momentum (GeV). For fixed-energy runs, equals gun_momentum_max_gev."
        },
        "gun_momentum_max_gev": {
            "type": "number",
            "description": "Maximum particle gun momentum (GeV). For fixed-energy runs, equals gun_momentum_min_gev."
        },
        "gun_theta_min_deg": {
            "type": "number",
            "description": "Minimum polar angle (degrees) for particle gun angular distribution."
        },
        "gun_theta_max_deg": {
            "type": "number",
            "description": "Maximum polar angle (degrees) for particle gun angular distribution."
        },
        "gun_phi_min_deg": {
            "type": "number",
            "description": "Minimum azimuthal angle (degrees) for particle gun distribution. Default is 0."
        },
        "gun_phi_max_deg": {
            "type": "number",
            "description": "Maximum azimuthal angle (degrees) for particle gun distribution. Default is 360."
        },
        "gun_distribution": {
            "type": "string",
            "description": "Angular distribution type for particle gun.",
            "enum": ["uniform", "cos(theta)", "eta", "pseudorapidity", "ffbar"]
        },
        "requester_dsc": {
            "type": "string",
            "description": "Detector Subsystem Collaboration requesting the dataset. Optional.",
            "enum": [
                "tracking",
                "other"
            ]
        },
        "generator": {
            "type": "string",
            "description": "Generator name",
            "enum": [
                "pythia6",
                "pythia8",
                "beagle",
                "djangoh",
                "rapgap",
                "dempgen",
                "sartre",
                "lager",
                "estarlight",
                "epic",
                "getalm",
                "eicmesonsfgen",
                "eic_sr_geant4",
                "eic_esr_xsuite",
                "sherpa",
                "single_particle",
                "other"
            ]
        },
    },
    "required": [
        "software_release",
        "is_background_mixed",
        "data_level",
        "geometry_config",
        "generator"
    ]
}


def validate_metadata(metadata: Dict[str, Any]) -> bool:
    """
    Validate metadata against the schema using jsonschema.
    
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

    try:
        json_validate(instance=metadata, schema=METADATA_SCHEMA)
    except ValidationError as e:
        raise ValueError(f"Metadata validation failed: {e.message}")

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
        with open(filepath, "r") as f:
            metadata = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in metadata file: {e}")

    validate_metadata(metadata)
    return metadata


# --------------------------------------------------------------------------- #
# Retry helper (exponential backoff + full jitter)
# --------------------------------------------------------------------------- #
TRANSIENT_EXC: Tuple[type, ...] = (DatabaseException, RSEWriteBlocked)
IDEMPOTENT_OK: Tuple[type, ...] = (
    DataIdentifierAlreadyExists, DuplicateContent, FileAlreadyExists,
    FileReplicaAlreadyExists, DuplicateRule,
)


def with_retries(fn: Callable[[], Any], *, what: str, logger: logging.Logger,
                 max_attempts: int = 5, base_delay: float = 3.0, max_delay: float = 60.0,
                 transient: Tuple[type, ...] = TRANSIENT_EXC,
                 swallow: Tuple[type, ...] = IDEMPOTENT_OK) -> Any:
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn()
        except swallow as e:
            logger.info("%s: already present (%s) -> ok", what, type(e).__name__)
            return None
        except transient as e:
            if attempt >= max_attempts:
                logger.error("%s: giving up after %d attempts (%s)", what, attempt, e)
                raise
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay += random.uniform(0, delay)
            logger.warning("%s: transient (%s) attempt %d/%d, retry in %.1fs",
                           what, type(e).__name__, attempt, max_attempts, delay)
            time.sleep(delay)


# --------------------------------------------------------------------------- #
# RSE resolution: writable + deterministic
# --------------------------------------------------------------------------- #
def resolve_target_rses(client: Client, rse_expression: str,
                        logger: logging.Logger) -> List[str]:
    rses = [r["rse"] for r in client.list_rses(rse_expression)]
    if not rses:
        raise InvalidRSEExpression(f"'{rse_expression}' resolved to no RSEs")
    good: List[str] = []
    for rse in rses:
        try:
            info = client.get_rse(rse)
        except Exception as e:
            logger.warning("Cannot read RSE %s (%s) -> skip", rse, e)
            continue
        if not info.get("availability_write", True):
            logger.warning("RSE %s not writable -> skip", rse)
            continue
        if not info.get("deterministic", True):
            logger.warning("RSE %s non-deterministic -> skip (needs a PFN for "
                           "manual registration)", rse)
            continue
        good.append(rse)
    if not good:
        raise InvalidRSEExpression(f"'{rse_expression}' -> no writable deterministic RSEs")
    random.shuffle(good)
    return good


def get_storage_token(client: Client) -> Optional[str]:
    """Only a JWT (3-part) token is usable by rsemgr, matching UploadClient."""
    tok = getattr(client, "auth_token", None)
    return tok if tok and len(tok.split(".")) == 3 else None


def file_replica_dict(path: str, scope: str, did_name: str) -> Dict[str, Any]:
    return {"scope": scope, "name": did_name, "bytes": os.stat(path).st_size,
            "adler32": adler32(path), "md5": md5(path), "state": "A"}


def existing_did_matches(client: Client, scope: str, name: str,
                         bytes_: int, adler32_: str) -> bool:
    """Return True if a DID already exists AND matches our size+checksum.
    Return False if it does not exist. Raise if it exists but DIFFERS -- same
    name, different content is a real conflict and is never 'ok'."""
    try:
        meta = client.get_metadata(scope, name)
    except DataIdentifierNotFound:
        return False
    r_adler = str(meta.get("adler32") or "").lstrip("0")
    l_adler = str(adler32_ or "").lstrip("0")
    r_bytes = meta.get("bytes")
    mismatch = (bool(r_adler) and bool(l_adler) and r_adler != l_adler) or \
               (r_bytes is not None and int(r_bytes) != int(bytes_))
    if mismatch:
        raise RuntimeError(
            f"DID {scope}:{name} already exists with bytes={r_bytes} "
            f"adler32={meta.get('adler32')} but local file is bytes={bytes_} "
            f"adler32={adler32_}; refusing (checksum/size conflict).")
    return True


# --------------------------------------------------------------------------- #
# Physical deletion of a dark file (direct, via the storage protocol -- no
# tombstone, no reaper). Used only when add_replicas never succeeded.
# --------------------------------------------------------------------------- #
def physical_delete(client: Client, rse: str, scope: str, name: str,
                    logger: logging.Logger) -> bool:
    try:
        try:
            token = client.get_delete_token(rse, scope, name, domain="wan")
        except Exception as error:
            logger.warning("No delete token for %s:%s @ %s (%s); trying non-token auth",
                           scope, name, rse, error)
            token = None
        rse_settings = rsemgr.get_rse_info(rse, vo=client.vo)
        rsemgr.delete(rse_settings, [{"scope": scope, "name": name}],
                      domain="wan", auth_token=token, logger=logger.log)
        logger.info("Deleted dark file %s:%s from %s", scope, name, rse)
    except SourceNotFound:
        logger.info("Dark file %s:%s already absent from %s", scope, name, rse)
        return True
    except Exception as e:
        logger.error("FAILED to delete dark file %s:%s from %s (%s) -- FLAG FOR "
                     "A DARK-DATA SWEEPER", scope, name, rse, e)
        return False


# --------------------------------------------------------------------------- #
# Upload one file, PanDA style: no_register upload with per-RSE retry + failover,
# then manual add_replicas; on registration failure, delete the physical file.
# Returns the RSE it landed on, or raises on total failure.
# --------------------------------------------------------------------------- #
def upload_and_register(upload_client: UploadClient, client: Client, *, path: str,
                        scope: str, did_name: str, candidate_rses: Sequence[str],
                        rse_settings_cache: Dict[str, Dict[str, Any]],
                        logger: logging.Logger, max_attempts_per_rse: int,
                        base_delay: float, transfer_timeout: Optional[int],
                        do_register: bool) -> str:
    local_adler = adler32(path)
    if do_register:
        existing_did_matches(client, scope, did_name, os.stat(path).st_size, local_adler)

    last_err: Optional[Exception] = None
    for rse in candidate_rses:
        # ---- physical upload (pure data movement; skips if already there) ----
        uploaded = False
        for attempt in range(1, max_attempts_per_rse + 1):
            item = {"path": path, "rse": rse, "did_scope": scope,
                    "did_name": did_name, "no_register": True}
            if transfer_timeout:
                item["transfer_timeout"] = transfer_timeout
            try:
                logger.info("Upload %s -> %s (try %d/%d)", did_name, rse,
                            attempt, max_attempts_per_rse)
                upload_client.upload([item], ignore_availability=True)
                uploaded = True
                break
            except (NoFilesUploaded, NotAllFilesUploaded, RSEWriteBlocked,
                    RucioException) as e:
                last_err = e
                if isinstance(e, RSEWriteBlocked) or attempt >= max_attempts_per_rse:
                    break
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, base_delay)
                logger.warning("Upload failed on %s (%s), retry in %.1fs",
                               rse, type(e).__name__, delay)
                time.sleep(delay)

        if not uploaded:
            logger.warning("RSE %s failed for %s -> failover", rse, did_name)
            continue

        if not do_register:            # --noregister: pure data movement
            logger.info("OK (no register): %s -> %s", did_name, rse)
            return rse

        # ---- manual replica registration -------------------------------------
        replica = file_replica_dict(path, scope, did_name)
        already_exists = (DataIdentifierAlreadyExists, DuplicateContent,
                          FileAlreadyExists, FileReplicaAlreadyExists)
        try:
            # Do NOT blanket-swallow already-exists here (swallow=()): a replica
            # under this name that differs in checksum/size is a real error, not
            # an idempotent no-op. Transient DB errors are still retried.
            with_retries(lambda: client.add_replicas(rse=rse, files=[replica]),
                         what=f"add_replicas@{rse}", logger=logger, swallow=())
        except already_exists:
            # Name already registered -> OK only if it is the SAME file.
            # existing_did_matches() raises on a checksum/size mismatch. We do
            # NOT delete here: on a deterministic RSE the physical file belongs
            # to the pre-existing replica, not to this upload.
            existing_did_matches(client, scope, did_name,
                                 replica["bytes"], replica["adler32"])
            logger.info("Replica %s:%s already present and matches -> ok",
                        scope, did_name)
            return rse
        except Exception as e:
            # Genuine registration failure on a file WE just uploaded -> DARK
            # DATA. Delete the physical file now (PanDA-style); do not fail over
            # (registration failure is a catalog problem, not an RSE problem).
            logger.error("add_replicas failed for %s:%s @ %s (%s) -> deleting dark file",
                         scope, did_name, rse, e)
            physical_delete(client, rse, scope, did_name, logger)
            raise RuntimeError(f"registration failed for {scope}:{did_name} @ {rse}: {e}")
        logger.info("Registered replica %s:%s @ %s", scope, did_name, rse)
        return rse

    raise RuntimeError(f"All RSEs failed for {scope}:{did_name}: {last_err}")


# --------------------------------------------------------------------------- #
# Decoupled, idempotent dataset ops
# --------------------------------------------------------------------------- #
def ensure_dataset(client: Client, scope: str, name: str,
                   meta: Optional[Dict[str, Any]], logger: logging.Logger) -> None:
    with_retries(lambda: client.add_dataset(scope=scope, name=name),
                 what=f"add_dataset {scope}:{name}", logger=logger)
    if meta:
        try:
            with_retries(lambda: client.set_metadata_bulk(scope=scope, name=name, meta=meta),
                         what=f"set_metadata {scope}:{name}", logger=logger)
        except Exception as e:
            logger.warning("Could not set metadata on %s:%s (%s)", scope, name, e)


def attach_files(client: Client, scope: str, name: str,
                 file_dids: List[Dict[str, str]], logger: logging.Logger) -> None:
    with_retries(lambda: client.add_files_to_datasets(
                    [{"scope": scope, "name": name, "dids": file_dids}],
                    ignore_duplicate=True),
                 what=f"attach -> {scope}:{name}", logger=logger)


def ensure_rule(client: Client, scope: str, name: str, rse_expression: str,
                copies: int, grouping: str, lifetime: Optional[int],
                logger: logging.Logger) -> None:
    def _add():
        rid = client.add_replication_rule(
            dids=[{"scope": scope, "name": name}], copies=copies,
            rse_expression=rse_expression, grouping=grouping, lifetime=lifetime)
        logger.info("Rule %s on %s:%s (copies=%d grouping=%s expr=%s)",
                    rid, scope, name, copies, grouping, rse_expression)
    with_retries(_add, what=f"add_rule {scope}:{name}", logger=logger)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> int:
    parser = argparse.ArgumentParser(
        prog="Register to RUCIO",
        description="Registers files to RUCIO with optional dataset metadata.",
    )
    parser.add_argument(
        "-f",
        dest="file_paths",
        action="store",
        nargs="+",
        required=True,
        help="Enter the local file path(s)",
    )
    parser.add_argument(
        "-d",
        dest="did_names",
        action="store",
        nargs="+",
        required=True,
        help="Enter the data identifier(s) for RUCIO catalogue",
    )
    parser.add_argument(
        "-s",
        dest="scope",
        action="store",
        required=True,
        help="Enter the scope",
    )
    parser.add_argument(
        "-r",
        dest="rse",
        action="store",
        required=True,
        help="Enter the RUCIO storage element (e.g., EIC-XRD for production outputs)",
    )
    parser.add_argument(
        "--noregister",
        dest="noregister",
        action="store_true",
        default=False,
        help="Skip RUCIO registration (upload only)",
    )
    parser.add_argument(
        "--upload-metadata",
        dest="metadata_file",
        action="store",
        default=None,
        help="Path to JSON file containing dataset metadata",
    )
    parser.add_argument(
        "--metadata-json",
        dest="metadata_json",
        action="store",
        default=None,
        help="JSON string containing dataset metadata",
    )
    parser.add_argument(
        "--distribute",
        choices=["pack", "spread"],
        default="spread",
        help="pack: all files to one RSE. spread: round-robin across the "
             "expression. Default: spread",
    )
    parser.add_argument(
        "--grouping",
        choices=["DATASET", "ALL", "NONE"],
        default=None,
        help="Rule grouping. Default: DATASET for pack, NONE for spread",
    )
    parser.add_argument("--copies", type=int, default=1)
    parser.add_argument(
        "--no-rule",
        dest="make_rule",
        action="store_false",
        default=True,
    )
    parser.add_argument("--lifetime", type=int, default=None)
    parser.add_argument("--max-attempts-per-rse", type=int, default=3)
    parser.add_argument("--base-delay", type=float, default=5.0)
    parser.add_argument("--transfer-timeout", type=int, default=None)

    args = parser.parse_args()

    logger = logging.getLogger("rucio_register")
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    if len(args.file_paths) != len(args.did_names):
        raise ValueError("Number of file paths must match number of DID names.")

    for file_path in args.file_paths:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

    for did in args.did_names:
        if not os.path.dirname(did):
            raise ValueError(
                f"DID '{did}' has no parent dataset (expected 'dataset/filename')."
            )

    if args.metadata_file and args.metadata_json:
        raise ValueError("Cannot specify both --upload-metadata and --metadata-json")
    dataset_meta: Optional[Dict[str, Any]] = None
    if args.metadata_file:
        dataset_meta = load_metadata_file(args.metadata_file)
    elif args.metadata_json:
        try:
            dataset_meta = json.loads(args.metadata_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in --metadata-json: {e}")
        validate_metadata(dataset_meta)

    grouping = args.grouping or ("NONE" if args.distribute == "spread" else "DATASET")
    if args.distribute == "spread" and grouping in ("DATASET", "ALL"):
        raise ValueError(
            f"placement 'spread' with grouping '{grouping}' makes the rule daemon "
            "consolidate all files onto one RSE -> many transfers "
            "(use --grouping NONE or --distribute pack)"
        )

    client = Client()
    upload_client = UploadClient(_client=client, logger=logger)
    rse_settings_cache: Dict[str, Dict[str, Any]] = {}

    candidates = resolve_target_rses(client, args.rse, logger)
    logger.info("RSE expression '%s' -> %s", args.rse, candidates)

    do_register = not args.noregister

    # --- Phase 1: upload (+ register replica unless --noregister) --------------
    placed: List[Tuple[str, str, str]] = []   # (did, dataset, rse)
    failures: List[str] = []
    for i, (path, did) in enumerate(zip(args.file_paths, args.did_names)):
        dataset_name = os.path.dirname(did)
        if args.distribute == "spread":
            k = i % len(candidates)
            order = candidates[k:] + candidates[:k]
        else:
            order = candidates
        try:
            rse = upload_and_register(
                upload_client, client, path=path, scope=args.scope, did_name=did,
                candidate_rses=order, rse_settings_cache=rse_settings_cache,
                logger=logger, max_attempts_per_rse=args.max_attempts_per_rse,
                base_delay=args.base_delay, transfer_timeout=args.transfer_timeout,
                do_register=do_register)
            placed.append((did, dataset_name, rse))
        except Exception as e:
            logger.error("FAILED file %s: %s", did, e)
            failures.append(did)

    if args.noregister:
        logger.info("--noregister: uploaded %d/%d, no catalog ops.",
                    len(placed), len(args.file_paths))
        return 1 if failures else 0

    # --- Phase 2: decoupled dataset ops (idempotent + backoff) ----------------
    datasets: Dict[str, List[Dict[str, str]]] = {}
    for did, dataset_name, _ in placed:
        datasets.setdefault(dataset_name, []).append({"scope": args.scope, "name": did})
    for dataset_name, file_dids in datasets.items():
        try:
            ensure_dataset(client, args.scope, dataset_name, dataset_meta, logger)
            attach_files(client, args.scope, dataset_name, file_dids, logger)
            if args.make_rule:
                ensure_rule(client, args.scope, dataset_name, rse_expression=args.rse,
                            copies=args.copies, grouping=grouping,
                            lifetime=args.lifetime, logger=logger)
        except Exception as e:
            # Replicas are registered (NOT dark). Any left unattached are ORPHANED
            # replicas -- reconciled by a separate process outside this script.
            # We just log and mark the files as not fully done.
            logger.error("Dataset ops failed for %s -> replicas registered but may be "
                         "ORPHANED (registered, not attached); leave for external "
                         "reconcile. Error: %s", dataset_name, e)
            failures.extend(d["name"] for d in file_dids)

    failed = set(failures)
    logger.info("Summary: %d ok, %d failed.", len(placed) - len(failed), len(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
