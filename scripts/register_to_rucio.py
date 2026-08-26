#!/usr/bin/env python3
"""Register files to Rucio with RSE failover, retries, and atomic registration.

Flow per file: upload with no_register (pure data movement, with per-RSE retry
and cross-RSE failover), then register the replica and attach it to its dataset
in a single server transaction via add_files_to_datasets() with an ``rse`` in
the attachment. Because register and attach share one transaction, there is no
registered-but-unattached ("orphaned") replica: either both commit or neither
does. A genuine registration failure leaves the uploaded bytes dark, so they are
deleted immediately (no reliance on the reaper). A same-name/different-content
DID is refused as a conflict and never deleted.

grouping is a rule property: DATASET/ALL keep a dataset's files on one RSE, NONE
spreads them. Placement (--distribute) and grouping must agree or the rule daemon
generates consolidation transfers; an inconsistent combination is refused.
"""

import argparse
import json
import logging
import os
import random
import sys
import time
from typing import Any, Callable, Optional, Sequence, Tuple

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

# Metadata plugin backing dataset custom tags in this deployment.
DATASET_META_PLUGIN = "POSTGRES_JSON"

# Errors worth retrying (transient) vs. errors that mean "already done" (ok).
TRANSIENT_EXC: Tuple[type, ...] = (DatabaseException, RSEWriteBlocked)
IDEMPOTENT_OK: Tuple[type, ...] = (
    DataIdentifierAlreadyExists, DuplicateContent, FileAlreadyExists,
    FileReplicaAlreadyExists, DuplicateRule,
)

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


def validate_metadata(metadata: dict[str, Any]) -> bool:
    """Validate a metadata dict against METADATA_SCHEMA.

    Args:
        metadata: The metadata dictionary to validate.

    Returns:
        True if valid.

    Raises:
        ValueError: If it is not a dict or does not match the schema.
    """
    if not isinstance(metadata, dict):
        raise ValueError("Metadata must be a JSON object (dictionary)")
    try:
        json_validate(instance=metadata, schema=METADATA_SCHEMA)
    except ValidationError as e:
        raise ValueError(f"Metadata validation failed: {e.message}")
    return True


def load_metadata_file(filepath: str) -> dict[str, Any]:
    """Load and validate dataset metadata from a JSON file.

    Args:
        filepath: Path to the JSON metadata file.

    Returns:
        The validated metadata dictionary.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the JSON is invalid or fails schema validation.
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


def apply_attempt_suffix(did_name: str, attempt: str) -> str:
    """Insert an attempt-number suffix before the file extension.
 
    e.g. 'RECO/dir/myfile.root', '2' -> 'RECO/dir/myfile_2.root'. Each attempt
    maps to a distinct DID and deterministic PFN, so a dark leftover from a
    failed prior attempt can never be adopted (skipped-over) by a later one.
 
    Args:
        did_name: DID name ('dataset/filename').
        attempt: Attempt number (string) to append.
 
    Returns:
        The DID name with '_<attempt>' inserted before the extension.
    """
    parent = os.path.dirname(did_name)
    root, ext = os.path.splitext(os.path.basename(did_name))
    new_base = f"{root}_{attempt}{ext}"
    return f"{parent}/{new_base}" if parent else new_base


def with_retries(
    fn: Callable[[], Any],
    *,
    what: str,
    logger: logging.Logger,
    max_attempts: int = 5,
    base_delay: float = 3.0,
    max_delay: float = 60.0,
    transient: Tuple[type, ...] = TRANSIENT_EXC,
    swallow: Tuple[type, ...] = IDEMPOTENT_OK,
) -> Any:
    """Call ``fn`` with exponential backoff and full jitter.

    Args:
        fn: Zero-argument callable to invoke.
        what: Short description used in log messages.
        logger: Logger for progress and failures.
        max_attempts: Maximum number of attempts before giving up.
        base_delay: Base backoff seconds; doubles each attempt.
        max_delay: Upper bound on the (pre-jitter) backoff.
        transient: Exceptions that trigger a retry.
        swallow: Exceptions treated as success (returns None).

    Returns:
        Whatever ``fn`` returns, or None if a ``swallow`` exception was caught.

    Raises:
        Exception: The last transient error if attempts are exhausted, or any
            exception not in ``transient``/``swallow``.
    """
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


def _rse_free_bytes(client: Client, rse: str) -> Optional[int]:
    """Return free bytes on ``rse``.

    Prefers the real backend 'storage' usage source and falls back to 'rucio'
    (sum of registered replicas).

    Args:
        client: Rucio client.
        rse: RSE name.

    Returns:
        Free bytes, or None if the RSE publishes no usable usage record.
    """
    usages = {u.get("source"): u for u in client.get_rse_usage(rse)}
    for src in ("storage", "rucio"):
        u = usages.get(src)
        if not u:
            continue
        free = u.get("free")
        if free is None and u.get("total") is not None and u.get("used") is not None:
            free = u["total"] - u["used"]
        if free is not None:
            return int(free)
    return None


def _account_remaining_bytes(client: Client, account: str, rse: str) -> Optional[float]:
    """Return the account's remaining quota on ``rse``.

    Args:
        client: Rucio client.
        account: Account name whose quota is consulted.
        rse: RSE name.

    Returns:
        Remaining bytes (limit minus used); float('inf') if no limit is set
        (unlimited); None if it cannot be determined.
    """
    try:
        recs = list(client.get_local_account_usage(account, rse))
    except Exception:
        return None
    if recs:
        rem = recs[0].get("bytes_remaining")
        return float("inf") if rem is None else int(rem)
    try:
        lim = client.get_local_account_limit(account, rse)
        b = lim.get(rse) if isinstance(lim, dict) else lim
        return float("inf") if b is None else int(b)
    except Exception:
        return float("inf")


def _rank_by_capacity(
    client: Client,
    rses: list[str],
    select: str,
    account: Optional[str],
    logger: logging.Logger,
) -> list[str]:
    """Order RSEs largest-capacity-first.

    Unknown-capacity RSEs sort last but remain (so they can still serve as
    failover); equal scores are broken randomly so a burst of jobs does not all
    pick the single largest RSE.

    Args:
        client: Rucio client.
        rses: Candidate RSE names.
        select: 'free' (RSE free space), 'quota' (account remaining quota), or
            'random' (shuffle).
        account: Account name, required for 'quota'.
        logger: Logger for the probed capacities.

    Returns:
        The RSEs ordered by descending capacity (or shuffled for 'random').
    """
    if select == "random" or len(rses) <= 1:
        random.shuffle(rses)
        return rses
    scores: dict[str, float] = {}
    for rse in rses:
        try:
            value = (_account_remaining_bytes(client, account, rse) if select == "quota"
                     else _rse_free_bytes(client, rse))
        except Exception as e:
            logger.warning("Capacity probe failed for %s (%s) -> deprioritise", rse, e)
            value = None
        scores[rse] = -1.0 if value is None else float(value)
        logger.info("RSE %s %s=%s", rse, select,
                    "unknown" if value is None else
                    ("unlimited" if value == float("inf") else int(value)))
    return sorted(rses, key=lambda r: (scores[r], random.random()), reverse=True)


def resolve_target_rses(
    client: Client,
    rse_expression: str,
    logger: logging.Logger,
    select: str = "free",
    account: Optional[str] = None,
) -> list[str]:
    """Resolve an RSE expression to an ordered list of usable RSEs.

    Keeps only writable, deterministic RSEs (manual registration needs no PFN
    only on deterministic RSEs) and orders them by capacity.

    Args:
        client: Rucio client.
        rse_expression: RSE name or expression.
        logger: Logger for skipped RSEs and capacities.
        select: Capacity metric for ordering ('free', 'quota', or 'random').
        account: Account name, used when select == 'quota'.

    Returns:
        Ordered list of candidate RSE names.

    Raises:
        InvalidRSEExpression: If nothing resolves to a writable deterministic RSE.
    """
    rses = [r["rse"] for r in client.list_rses(rse_expression)]
    if not rses:
        raise InvalidRSEExpression(f"'{rse_expression}' resolved to no RSEs")
    good: list[str] = []
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
            logger.warning("RSE %s non-deterministic -> skip (needs a PFN)", rse)
            continue
        good.append(rse)
    if not good:
        raise InvalidRSEExpression(f"'{rse_expression}' -> no writable deterministic RSEs")
    return _rank_by_capacity(client, good, select, account, logger)


class ChecksumConflict(RuntimeError):
    """A DID with this scope:name already exists but with different content."""


def existing_did_matches(
    client: Client,
    scope: str,
    name: str,
    bytes_: int,
    adler32_: str,
) -> bool:
    """Check whether an existing DID matches the local file.

    Args:
        client: Rucio client.
        scope: DID scope.
        name: DID name.
        bytes_: Local file size in bytes.
        adler32_: Local file adler32 checksum.

    Returns:
        True if the DID exists and matches size+checksum; False if it does not
        exist.

    Raises:
        ChecksumConflict: If the DID exists but differs in size or checksum.
    """
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
        raise ChecksumConflict(
            f"DID {scope}:{name} already exists with bytes={r_bytes} "
            f"adler32={meta.get('adler32')} but local file is bytes={bytes_} "
            f"adler32={adler32_}; refusing (checksum/size conflict).")
    return True


def physical_delete(
    client: Client,
    rse: str,
    scope: str,
    name: str,
    logger: logging.Logger,
) -> bool:
    """Delete a dark file directly from storage (no tombstone, no reaper).

    Requests a scoped delete token from the server and removes the deterministic
    final path via the storage protocol. Used only when registration never
    committed, so the bytes belong to no catalog entry.

    Args:
        client: Rucio client.
        rse: RSE holding the dark file.
        scope: DID scope.
        name: DID name.
        logger: Logger for the outcome.

    Returns:
        True if the file was deleted or already absent; False on failure.
    """
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
        return True
    except SourceNotFound:
        logger.info("Dark file %s:%s already absent from %s", scope, name, rse)
        return True
    except Exception as e:
        logger.error("FAILED to delete dark file %s:%s from %s (%s) -- FLAG FOR "
                     "A DARK-DATA SWEEPER", scope, name, rse, e)
        return False


def upload_file(
    upload_client: UploadClient,
    *,
    path: str,
    scope: str,
    did_name: str,
    candidate_rses: Sequence[str],
    logger: logging.Logger,
    max_attempts_per_rse: int,
    base_delay: float,
    transfer_timeout: Optional[int],
) -> str:
    """Upload one file with no_register, retrying per RSE and failing over.

    Pure data movement: no catalog writes. Each RSE is retried with backoff; a
    write-blocked RSE is skipped immediately.

    Args:
        upload_client: Configured UploadClient.
        path: Local file path.
        scope: DID scope.
        did_name: DID name ('dataset/filename').
        candidate_rses: RSEs to try, in order.
        logger: Logger for progress.
        max_attempts_per_rse: Attempts before failing over to the next RSE.
        base_delay: Base backoff seconds between attempts on one RSE.
        transfer_timeout: Optional per-transfer timeout in seconds.

    Returns:
        The RSE the file landed on.

    Raises:
        RuntimeError: If every candidate RSE fails.
    """
    last_err: Optional[Exception] = None
    for rse in candidate_rses:
        for attempt in range(1, max_attempts_per_rse + 1):
            item = {"path": path, "rse": rse, "did_scope": scope,
                    "did_name": did_name, "no_register": True}
            if transfer_timeout:
                item["transfer_timeout"] = transfer_timeout
            try:
                logger.info("Upload %s -> %s (try %d/%d)", did_name, rse,
                            attempt, max_attempts_per_rse)
                upload_client.upload([item], ignore_availability=True)
                logger.info("Uploaded %s -> %s", did_name, rse)
                return rse
            except (NoFilesUploaded, NotAllFilesUploaded, RSEWriteBlocked,
                    RucioException) as e:
                last_err = e
                if isinstance(e, RSEWriteBlocked) or attempt >= max_attempts_per_rse:
                    break
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, base_delay)
                logger.warning("Upload failed on %s (%s), retry in %.1fs",
                               rse, type(e).__name__, delay)
                time.sleep(delay)
        logger.warning("RSE %s failed for %s -> failover", rse, did_name)
    raise RuntimeError(f"All RSEs failed for {scope}:{did_name}: {last_err}")


def register_and_attach(
    client: Client,
    *,
    scope: str,
    did_name: str,
    dataset: str,
    rse: str,
    bytes_: int,
    adler32_: str,
    md5_: Optional[str],
    logger: logging.Logger,
) -> None:
    """Register a replica and attach it to its dataset in one transaction.

    Uses a single add_files_to_datasets() call with ``rse`` in the attachment so
    the replica insert and the dataset attach commit or roll back together --
    there is no registered-but-unattached orphan. The dataset must already exist.

    Args:
        client: Rucio client.
        scope: DID scope (of both file and dataset).
        did_name: File DID name.
        dataset: Dataset name to attach to.
        rse: RSE holding the uploaded replica.
        bytes_: File size in bytes.
        adler32_: File adler32 checksum.
        md5_: File md5 checksum, if available.
        logger: Logger for the outcome.

    Raises:
        ChecksumConflict: If the DID exists with different content (not deleted).
        RuntimeError: On genuine registration failure (the dark file is deleted).
    """
    already_exists = (DataIdentifierAlreadyExists, DuplicateContent,
                      FileAlreadyExists, FileReplicaAlreadyExists)
    did: dict[str, Any] = {"scope": scope, "name": did_name,
                           "bytes": bytes_, "adler32": adler32_}
    if md5_:
        did["md5"] = md5_
    attachment = [{"scope": scope, "name": dataset, "rse": rse, "dids": [did]}]
    try:
        with_retries(lambda: client.add_files_to_datasets(attachment, ignore_duplicate=True),
                     what=f"register+attach {scope}:{did_name} -> {dataset}@{rse}",
                     logger=logger, swallow=())
        logger.info("Registered+attached %s:%s -> %s @ %s", scope, did_name, dataset, rse)
    except already_exists:
        existing_did_matches(client, scope, did_name, bytes_, adler32_)
        logger.info("%s:%s already registered+attached and matches -> ok", scope, did_name)
    except Exception as e:
        logger.error("register+attach failed for %s:%s @ %s (%s) -> deleting dark file",
                     scope, did_name, rse, e)
        physical_delete(client, rse, scope, did_name, logger)
        raise RuntimeError(f"register+attach failed for {scope}:{did_name} @ {rse}: {e}")


def ensure_dataset(
    client: Client,
    scope: str,
    name: str,
    meta: Optional[dict[str, Any]],
    logger: logging.Logger,
) -> None:
    """Create the dataset if missing and set metadata only if not already present.

    The dataset is created without a rule (the rule is added once, separately).
    Existing metadata is read via the POSTGRES_JSON plugin so a dataset created
    by an earlier job is not overwritten; only missing keys are set.

    Args:
        client: Rucio client.
        scope: Dataset scope.
        name: Dataset name.
        meta: Metadata to apply, or None.
        logger: Logger for the outcome.
    """
    with_retries(lambda: client.add_dataset(scope=scope, name=name),
                 what=f"add_dataset {scope}:{name}", logger=logger)
    if not meta:
        return
    try:
        existing = client.get_metadata(scope, name, plugin=DATASET_META_PLUGIN)
    except Exception as e:
        existing = {}
        logger.warning("Could not read %s metadata on %s:%s (%s); will set",
                       DATASET_META_PLUGIN, scope, name, e)
    to_set = {k: v for k, v in meta.items() if k not in (existing or {})}
    if not to_set:
        logger.info("Dataset %s:%s already has metadata -> not adding", scope, name)
        return
    try:
        with_retries(lambda: client.set_metadata_bulk(scope=scope, name=name, meta=to_set),
                     what=f"set_metadata {scope}:{name}", logger=logger)
    except Exception as e:
        logger.warning("Could not set metadata on %s:%s (%s)", scope, name, e)


def ensure_rule(
    client: Client,
    scope: str,
    name: str,
    rse_expression: str,
    copies: int,
    grouping: str,
    lifetime: Optional[int],
    logger: logging.Logger,
) -> None:
    """Create one replication rule on the dataset, tolerating duplicates.

    Args:
        client: Rucio client.
        scope: Dataset scope.
        name: Dataset name.
        rse_expression: RSE expression the rule targets.
        copies: Number of copies.
        grouping: Rule grouping ('DATASET', 'ALL', or 'NONE').
        lifetime: Rule lifetime in seconds, or None for permanent.
        logger: Logger for the outcome.
    """
    def _add():
        rid = client.add_replication_rule(
            dids=[{"scope": scope, "name": name}], copies=copies,
            rse_expression=rse_expression, grouping=grouping, lifetime=lifetime)
        logger.info("Rule %s on %s:%s (copies=%d grouping=%s expr=%s)",
                    rid, scope, name, copies, grouping, rse_expression)
    with_retries(_add, what=f"add_rule {scope}:{name}", logger=logger)


def main() -> int:
    """Parse arguments, upload files, and register them with Rucio.

    Returns:
        0 if every file succeeded, 1 if any failed.
    """
    parser = argparse.ArgumentParser(
        prog="Register to RUCIO",
        description="Registers files to RUCIO with optional dataset metadata.",
    )
    parser.add_argument(
        "-f",
        dest="file_paths",
        nargs="+",
        required=True,
        help="Local file path(s)",
    )
    parser.add_argument(
        "-d",
        dest="did_names",
        nargs="+",
        required=True,
        help="Data identifier(s) for the RUCIO catalogue ('dataset/filename')",
    )
    parser.add_argument(
        "-s",
        dest="scope",
        required=True,
        help="Scope",
    )
    parser.add_argument(
        "-r",
        dest="rse",
        required=True,
        help="RSE name or expression (e.g. EIC-XRD for production outputs)",
    )
    parser.add_argument(
        "--noregister",
        dest="noregister",
        action="store_true",
        default=False,
        help="Upload only; skip catalog registration",
    )
    parser.add_argument(
        "--upload-metadata",
        dest="metadata_file",
        default=None,
        help="Path to a JSON file with dataset metadata",
    )
    parser.add_argument(
        "--metadata-json",
        dest="metadata_json",
        default=None,
        help="JSON string with dataset metadata",
    )
    parser.add_argument(
        "--distribute",
        choices=["pack", "spread"],
        default="spread",
        help="pack: all files to one RSE; spread: round-robin across the expression",
    )
    parser.add_argument(
        "--select",
        choices=["free", "quota", "random"],
        default="free",
        help="RSE priority when several resolve: free = most RSE free space; "
             "quota = most remaining account quota; random = shuffle",
    )
    parser.add_argument(
        "--grouping",
        choices=["DATASET", "ALL", "NONE"],
        default=None,
        help="Rule grouping; default DATASET for pack, NONE for spread",
    )
    parser.add_argument(
        "--copies",
        type=int,
        default=1,
        help="Number of copies for the dataset rule",
    )
    parser.add_argument(
        "--no-rule",
        dest="make_rule",
        action="store_false",
        default=True,
        help="Do not create a dataset replication rule",
    )
    parser.add_argument(
        "--lifetime",
        type=int,
        default=None,
        help="Rule lifetime in seconds",
    )
    parser.add_argument(
        "--max-attempts-per-rse",
        type=int,
        default=3,
        help="Upload attempts per RSE before failing over",
    )
    parser.add_argument(
        "--base-delay",
        type=float,
        default=5.0,
        help="Base backoff seconds between upload attempts",
    )
    parser.add_argument(
        "--transfer-timeout",
        type=int,
        default=None,
        help="Per-transfer timeout in seconds",
    )
    parser.add_argument(
        "--attempt",
        default=os.environ.get("AttemptNr"),
        type=int
        help="Attempt number appended to each file DID name before the extension "
             "(myfile.root -> myfile_<N>.root) so every retry writes a distinct "
             "DID and PFN. Defaults to $PanDA_AttemptNr.",
    )
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
            raise ValueError(f"DID '{did}' has no parent dataset (expected 'dataset/filename').")
    if args.attempt is None:
        parser.error("--attempt is required (pass --attempt N or set $AttemptNr)")

    did_names = [apply_attempt_suffix(d, str(args.attempt)) for d in args.did_names]

    if args.metadata_file and args.metadata_json:
        raise ValueError("Cannot specify both --upload-metadata and --metadata-json")
    dataset_meta: Optional[dict[str, Any]] = None
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
            "(use --grouping NONE or --distribute pack)")

    client = Client()
    upload_client = UploadClient(_client=client, logger=logger)

    candidates = resolve_target_rses(client, args.rse, logger,
                                     select=args.select, account=client.account)
    logger.info("RSE expression '%s' -> %s", args.rse, candidates)

    do_register = not args.noregister

    # Phase 1: upload only (no catalog writes). placed: (did, dataset, rse, bytes, adler32, md5)
    placed: list[Tuple[str, str, str, int, str, str]] = []
    failures: list[str] = []
    for i, (path, did) in enumerate(zip(args.file_paths, did_names)):
        dataset_name = os.path.dirname(did)
        if args.distribute == "spread":
            k = i % len(candidates)
            order = candidates[k:] + candidates[:k]
        else:
            order = candidates

        if do_register:
            try:
                existing_did_matches(client, args.scope, did,
                                     os.stat(path).st_size, adler32(path))
            except ChecksumConflict as e:
                logger.error("FAILED file %s: %s", did, e)
                failures.append(did)
                continue

        try:
            rse = upload_file(upload_client, path=path, scope=args.scope, did_name=did,
                              candidate_rses=order, logger=logger,
                              max_attempts_per_rse=args.max_attempts_per_rse,
                              base_delay=args.base_delay,
                              transfer_timeout=args.transfer_timeout)
        except Exception as e:
            logger.error("FAILED upload %s: %s", did, e)
            failures.append(did)
            continue

        placed.append((did, dataset_name, rse,
                       os.stat(path).st_size, adler32(path), md5(path)))

    if args.noregister:
        logger.info("--noregister: uploaded %d/%d, no catalog ops.",
                    len(placed), len(args.file_paths))
        return 1 if failures else 0

    # Phase 2: per dataset, ensure it exists, then atomic register+attach, then one rule.
    datasets: dict[str, list[Tuple[str, str, int, str, str]]] = {}
    for did, dataset_name, rse, b, a, m in placed:
        datasets.setdefault(dataset_name, []).append((did, rse, b, a, m))

    for dataset_name, items in datasets.items():
        try:
            ensure_dataset(client, args.scope, dataset_name, dataset_meta, logger)
        except Exception as e:
            logger.error("ensure_dataset failed for %s (%s) -> deleting %d dark file(s)",
                         dataset_name, e, len(items))
            for did, rse, *_ in items:
                physical_delete(client, rse, args.scope, did, logger)
                failures.append(did)
            continue

        attached_any = False
        for did, rse, b, a, m in items:
            try:
                register_and_attach(client, scope=args.scope, did_name=did,
                                    dataset=dataset_name, rse=rse,
                                    bytes_=b, adler32_=a, md5_=m, logger=logger)
                attached_any = True
            except ChecksumConflict as e:
                logger.error("FAILED file %s: %s", did, e)
                failures.append(did)
            except Exception as e:
                logger.error("FAILED register+attach %s: %s", did, e)
                failures.append(did)

        if args.make_rule and attached_any:
            try:
                ensure_rule(client, args.scope, dataset_name, rse_expression=args.rse,
                            copies=args.copies, grouping=grouping,
                            lifetime=args.lifetime, logger=logger)
            except Exception as e:
                logger.error("rule failed for %s (%s); files attached, rule deferred "
                             "to re-run", dataset_name, e)

    failed = set(failures)
    logger.info("Summary: %d ok, %d failed.", len(placed) - len(failed), len(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
