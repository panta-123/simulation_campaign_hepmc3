#!/bin/bash
set -Euo pipefail
trap 's=$?; echo "$0: Error on line "$LINENO": $BASH_COMMAND"; exit $s' ERR
IFS=$'\n\t'

# Load job environment (mask secrets)
if ls environment*.sh ; then
  grep -v BEARER environment*.sh
  source environment*.sh
fi

# Check arguments
if [ $# -lt 1 ] ; then
  echo "Usage: "
  echo "  $0 <input> [n_chunk=10000] [i_chunk=]"
  echo
  echo "A typical npsim run requires from 0.5 to 5 core-seconds per event,"
  echo "and uses under 3 GB of memory. The output ROOT file for"
  echo "10k events take up about 2 GB in disk space."
  exit
fi

# Startup
echo "date sys: $(date)"
echo "date web: $(date -d "$(curl --insecure --head --silent --max-redirs 0 google.com 2>&1 | grep Date: | cut -d' ' -f2-7)")"
echo "hostname: $(hostname -f)"
echo "uname:    $(uname -a)"
echo "whoami:   $(whoami)"
echo "pwd:      $(pwd)"
echo "site:     ${GLIDEIN_Site:-}"
echo "resource: ${GLIDEIN_ResourceName:-}"
echo "http_proxy: ${http_proxy:-}"
df -h --exclude-type=fuse --exclude-type=tmpfs
ls -al
test -f .job.ad && cat .job.ad
test -f .machine.ad && cat .machine.ad

# Load container environment (include ${DETECTOR_VERSION})
export DETECTOR_CONFIG_REQUESTED=${DETECTOR_CONFIG:-}
export DETECTOR_VERSION_REQUESTED=${DETECTOR_VERSION:-main}
source /opt/detector/epic-${DETECTOR_VERSION_REQUESTED}/bin/thisepic.sh
export DETECTOR_VERSION=${DETECTOR_VERSION_REQUESTED}
export DETECTOR_CONFIG=${DETECTOR_CONFIG_REQUESTED:-${DETECTOR_CONFIG:-$DETECTOR}}
export SCRIPT_DIR=$(realpath $(dirname $0))
export RUCIO_CONFIG=$SCRIPT_DIR/rucio.cfg
export RUCIO_ACCOUNT=eicprod

# Print out the location of the rucio config file
echo $RUCIO_CONFIG

# Argument parsing
# - input file basename
BASENAME=${1}
# - input file extension to determine type of simulation
EXTENSION=${2}
# - number of events
EVENTS_PER_TASK=${3:-10000}
# - current chunk (zero-based)
if [ ${#} -lt 4 ] ; then
  TASK=""
  SEED=1
  SKIP_N_EVENTS=0
else
  # 10-base input task number to 4-zero-padded task number
  TASK=".${4}"
  SEED=$((10#${4}+1))
  # assumes zero-based task number, can be zero-padded 
  SKIP_N_EVENTS=$((10#${4}*EVENTS_PER_TASK))
fi

# Output location
BASEDIR=${DATADIR:-${PWD}}

# XRD Write locations (allow for empty URL override)
XRDWURL=${XRDWURL-"xroots://dtn2201.jlab.org/"}
XRDWBASE=${XRDWBASE:-"/eic/eic2/EPIC"}

# XRD Read locations (allow for empty URL override)
XRDRURL=${XRDRURL-"root://dtn-eic.jlab.org/"}
XRDRBASE=${XRDRBASE:-"/volatile/eic/EPIC"}

# Local temp dir
echo "SLURM_TMPDIR=${SLURM_TMPDIR:-}"
echo "SLURM_JOB_ID=${SLURM_JOB_ID:-}"
echo "SLURM_ARRAY_JOB_ID=${SLURM_ARRAY_JOB_ID:-}"
echo "SLURM_ARRAY_TASK_ID=${SLURM_ARRAY_TASK_ID:-}"
echo "_CONDOR_SCRATCH_DIR=${_CONDOR_SCRATCH_DIR:-}"
echo "OSG_WN_TMP=${OSG_WN_TMP:-}"
if [ -n "${_CONDOR_SCRATCH_DIR:-}" ] ; then
  TMPDIR=${_CONDOR_SCRATCH_DIR}
elif [ -n "${SLURM_TMPDIR:-}" ] ; then
  TMPDIR=${SLURM_TMPDIR}
else
  if [ -d "/scratch/slurm/${SLURM_JOB_ID:-}" ] ; then
    TMPDIR="/scratch/slurm/${SLURM_JOB_ID:-}"
  else
    TMPDIR=${TMPDIR:-/tmp}/${$}
  fi
fi
echo "TMPDIR=${TMPDIR}"
mkdir -p ${TMPDIR}
ls -al ${TMPDIR}

# Input file parsing
INPUT_FILE=${BASENAME}.${EXTENSION}
TASKNAME=${TAG_SUFFIX:+${TAG_SUFFIX}_}$(basename ${BASENAME})${TASK}
INPUT_DIR=$(dirname $(realpath --canonicalize-missing --relative-to=${BASEDIR} ${INPUT_FILE}))
# - file.hepmc              -> TAG="", and avoid double // in S3 location
# - EVGEN/file.hepmc        -> TAG="", and avoid double // in S3 location
# - EVGEN/DIS/file.hepmc    -> TAG="DIS"
# - EVGEN/DIS/NC/file.hepmc -> TAG="DIS/NC"
# - ../file.hepmc           -> error
if [ ! "${INPUT_DIR/\.\.\//}" = "${INPUT_DIR}" ] ; then
  echo "Error: Input file must be below current directory."
  exit
fi
INPUT_PREFIX=${INPUT_DIR/\/*/}
TAG=${INPUT_DIR/${INPUT_PREFIX}\//}
INPUT_DIR=${BASEDIR}/EVGEN/${TAG}
mkdir -p ${INPUT_DIR}
TAG=${DETECTOR_VERSION:-main}/${DETECTOR_CONFIG}/${TAG_PREFIX:+${TAG_PREFIX}/}${TAG}

if [[ "$EXTENSION" == "hepmc3.tree.root" ]]; then
  # Define location on xrootd from where to stream input file from
  INPUT_FILE=${XRDRURL}/${XRDRBASE}/${INPUT_FILE}
else
  # Copy input file from xrootd
  xrdcp -f ${XRDRURL}/${XRDRBASE}/${INPUT_FILE} ${INPUT_DIR}
fi

# Output file names
LOG_DIR=LOG/${TAG}
LOG_TEMP=${TMPDIR}/${LOG_DIR}
mkdir -p ${LOG_TEMP} 
#
FULL_DIR=FULL/${TAG}
FULL_TEMP=${TMPDIR}/${FULL_DIR}
mkdir -p ${FULL_TEMP} 
#
RECO_DIR=RECO/${TAG}
RECO_TEMP=${TMPDIR}/${RECO_DIR}
mkdir -p ${RECO_TEMP} 

# Mix background events if the input file is a hepmc file
if [[ "$EXTENSION" == "hepmc3.tree.root" ]]; then
  BG_ARGS=()  

  SIGNAL_STATUS_VALUE=${SIGNAL_STATUS:-0}
  STABLE_STATUSES="$((${SIGNAL_STATUS_VALUE}+1))"
  DECAY_STATUSES="$((${SIGNAL_STATUS_VALUE}+2))"

  if [[ -n "${BG_FILES:-}" ]]; then
    while read -r bg_file; do
      file=$(echo "$bg_file" | jq -r '.file')
      freq=$(echo "$bg_file" | jq -r '.freq')
      skip=$(echo "$bg_file" | jq -r '.skip')
      
      # This ensures that the number of background events skipped before sampling from the source is atleast 1.
      skip=$(awk "BEGIN {print int((${SKIP_N_EVENTS}*${skip})+1)}")  
      status=$(echo "$bg_file" | jq -r '.status')
      BG_ARGS+=(--bgFile "$file" "$freq" "$skip" "$status")
      STABLE_STATUSES="${STABLE_STATUSES} $((status+1))"
      DECAY_STATUSES="${DECAY_STATUSES} $((status+2))"
    done < <(jq -c '.[]' ${BG_FILES})
    # Run the background merger with proper logging
    {
      date
      eic-info
      prmon \
        --filename ${LOG_TEMP}/${TASKNAME}.hepmcmerger.prmon.txt \
        --json-summary ${LOG_TEMP}/${TASKNAME}.hepmcmerger.prmon.json \
        -- \
      SignalBackgroundMerger \
        --rngSeed ${SEED:-1} \
        --nSlices ${EVENTS_PER_TASK} \
        --signalSkip ${SKIP_N_EVENTS} \
        --signalFile ${INPUT_FILE} \
        --signalFreq ${SIGNAL_FREQ:-0} \
        --signalStatus ${SIGNAL_STATUS:-0} \
        "${BG_ARGS[@]}" \
        --outputFile ${FULL_TEMP}/${TASKNAME}.hepmc3.tree.root

    } 2>&1 | tee ${LOG_TEMP}/${TASKNAME}.hepmcmerger.log | tail -n1000

    # Use background merged file as input for next stage
    INPUT_FILE=${FULL_TEMP}/${TASKNAME}.hepmc3.tree.root
    # Don't skip events on the background merged file
    SKIP_N_EVENTS=0
  else
    echo "No background mixing will be performed since no sources are provided"
  fi
else
  echo "No background mixing is performed for singles"
fi

# Run simulation
{
  date
  eic-info
  # Common flags shared by both types of simulation
  common_flags=(
    --random.seed ${SEED:-1}
    --random.enableEventSeed
    --printLevel WARNING
    --filter.tracker 'edep0'
    --numberOfEvents ${EVENTS_PER_TASK}
    --compactFile ${DETECTOR_PATH}/${DETECTOR_CONFIG}${EBEAM:+${PBEAM:+_${EBEAM}x${PBEAM}}}.xml
    --outputFile ${FULL_TEMP}/${TASKNAME}.edm4hep.root
  )
  # Uncommon flags based on EXTENSION
  if [[ "$EXTENSION" == "hepmc3.tree.root" ]]; then
    uncommon_flags=(
      --runType batch
      --skipNEvents ${SKIP_N_EVENTS}
      --hepmc3.useHepMC3 ${USEHEPMC3:-true}
      --physics.alternativeStableStatuses "${STABLE_STATUSES}"
      --physics.alternativeDecayStatuses "${DECAY_STATUSES}"
      --inputFiles ${INPUT_FILE}
    )
  else
    uncommon_flags=(
      --runType run
      --enableGun
      --steeringFile ${INPUT_FILE}
    )
  fi
  # Run npsim with both common and uncommon flags
  prmon \
    --filename ${LOG_TEMP}/${TASKNAME}.npsim.prmon.txt \
    --json-summary ${LOG_TEMP}/${TASKNAME}.npsim.prmon.json \
    --log-filename ${LOG_TEMP}/${TASKNAME}.npsim.prmon.log \
    -- \
  npsim "${common_flags[@]}" "${uncommon_flags[@]}"
  ls -al ${FULL_TEMP}/${TASKNAME}.edm4hep.root  
} 2>&1 | tee ${LOG_TEMP}/${TASKNAME}.npsim.log | tail -n1000

# Validate FULL event count against expected NEVENTS
echo "=== Validating FULL event count ==="
NEVENTS_FULL=$(python3 -c "import ROOT; ROOT.gErrorIgnoreLevel=ROOT.kError; f=ROOT.TFile.Open('${FULL_TEMP}/${TASKNAME}.edm4hep.root'); print(int(f.Get('events').GetEntries()))" 2>/dev/null)
if [ -z "${NEVENTS_FULL}" ]; then
  echo "ERROR: Could not read event count from FULL ROOT file."
  exit 66
fi
echo "FULL event count: ${NEVENTS_FULL} (expected: ${EVENTS_PER_TASK})"
if [ "${NEVENTS_FULL}" -ne "${EVENTS_PER_TASK}" ]; then
  echo "ERROR: FULL event count mismatch: got ${NEVENTS_FULL}, expected ${EVENTS_PER_TASK}."
  exit 66
fi

# Run eicrecon reconstruction
{
  date
  eic-info
  prmon \
    --filename ${LOG_TEMP}/${TASKNAME}.eicrecon.prmon.txt \
    --json-summary ${LOG_TEMP}/${TASKNAME}.eicrecon.prmon.json \
    --log-filename ${LOG_TEMP}/${TASKNAME}.eicrecon.prmon.log \
    -- \
  eicrecon \
    -Pdd4hep:xml_files="${DETECTOR_PATH}/${DETECTOR_CONFIG}${EBEAM:+${PBEAM:+_${EBEAM}x${PBEAM}}}.xml" \
    -Ppodio:output_file="${RECO_TEMP}/${TASKNAME}.eicrecon.edm4eic.root" \
    -Pjana:warmup_timeout=0 -Pjana:timeout=0 \
    -Pplugins=janadot \
    "${FULL_TEMP}/${TASKNAME}.edm4hep.root"
  if [ -f jana.dot ] ; then mv jana.dot ${LOG_TEMP}/${TASKNAME}.eicrecon.dot ; fi
  ls -al ${RECO_TEMP}/${TASKNAME}.eicrecon.edm4eic.root
} 2>&1 | tee ${LOG_TEMP}/${TASKNAME}.eicrecon.log | tail -n1000

# Validate RECO event count against expected NEVENTS
echo "=== Validating RECO event count ==="
NEVENTS_RECO=$(python3 -c "import ROOT; ROOT.gErrorIgnoreLevel=ROOT.kError; f=ROOT.TFile.Open('${RECO_TEMP}/${TASKNAME}.eicrecon.edm4eic.root'); print(int(f.Get('events').GetEntries()))" 2>/dev/null)
if [ -z "${NEVENTS_RECO}" ]; then
  echo "ERROR: Could not read event count from RECO ROOT file."
  exit 66
fi
echo "RECO event count: ${NEVENTS_RECO} (expected: ${EVENTS_PER_TASK})"
if [ "${NEVENTS_RECO}" -ne "${EVENTS_PER_TASK}" ]; then
  echo "ERROR: RECO event count mismatch: got ${NEVENTS_RECO}, expected ${EVENTS_PER_TASK}."
  exit 66
fi

# List log files
ls -al ${LOG_TEMP}/${TASKNAME}.*

# Build metadata JSON string for Rucio registration
PBEAM_ENERGY="${PBEAM%%_*}"
PBEAM_SPECIES="${PBEAM##*_}"
IS_BG_MIXED="false"
if [ -n "${BG_FILES:-}" ]; then IS_BG_MIXED="true"; fi
PHYSICS_PROCESS_JSON=$(python3 -c "import json,sys; print(json.dumps([s.strip() for s in sys.argv[1].split(',')]))" "${PHYSICS_PROCESS}")
METADATA_JSON="{\"software_release\": \"${JUG_XL_TAG}\", \"physics_process\": ${PHYSICS_PROCESS_JSON}, \"electron_beam_energy\": ${EBEAM}, \"ion_beam_energy\": ${PBEAM_ENERGY}, \"ion_species\": \"${PBEAM_SPECIES}\", \"is_background_mixed\": ${IS_BG_MIXED}, \"generator\": \"${GENERATOR}\", \"number_of_events\": ${EVENTS_PER_TASK}}"

# Data egress to directory

if [ "${COPYLOG:-false}" == "true" ] ; then
  if [ "${USERUCIO:-false}" == "true" ] ; then
    TIME_TAG=$(date --iso-8601=second)
    TARFILE="${LOG_TEMP}/${TASKNAME}.log.tar.gz"

    # Initialize an empty array to hold existing files
    FILES_TO_TAR=()

    # List of expected files
    for FILE in \
      "${LOG_TEMP}/${TASKNAME}.npsim.prmon.txt" \
      "${LOG_TEMP}/${TASKNAME}.npsim.log" \
      "${LOG_TEMP}/${TASKNAME}.eicrecon.prmon.txt" \
      "${LOG_TEMP}/${TASKNAME}.eicrecon.log" \
      "${LOG_TEMP}/${TASKNAME}.eicrecon.dot" \
      "${LOG_TEMP}/${TASKNAME}.hepmcmerger.log"
    do
      if [ -f "$FILE" ]; then
        FILES_TO_TAR+=("$FILE")
      fi
    done

    # Create the tar archive only if there are files to include
    if [ ${#FILES_TO_TAR[@]} -gt 0 ]; then
      tar -czvf "$TARFILE" "${FILES_TO_TAR[@]}"
    else
      echo "No log files found to archive."
    fi
    
    python $SCRIPT_DIR/register_to_rucio.py \
    -f "${LOG_TEMP}/${TASKNAME}.log.tar.gz" \
    -d "/${LOG_DIR}/${TASKNAME}.${TIME_TAG}.log.tar.gz" \
    -s epic -r ${LOG_RSE:-EIC-XRD-LOG} --noregister
  else
    # Token for write authentication
    echo "=== DEBUG: Attempting to copy LOG files to xrootd ==="
    echo "BEARER_TOKEN file location: ${_CONDOR_CREDS:-.}/eic.use"
    if [ -f "${_CONDOR_CREDS:-.}/eic.use" ]; then
      export BEARER_TOKEN=$(cat ${_CONDOR_CREDS:-.}/eic.use)
      echo "BEARER_TOKEN loaded successfully"
    else
      echo "WARNING: BEARER_TOKEN file not found at ${_CONDOR_CREDS:-.}/eic.use"
      if [ -f "x509_user_proxy" ]; then
        echo "Found x509_user_proxy, setting X509_USER_PROXY"
        export X509_USER_PROXY="x509_user_proxy"
      fi
    fi
    echo "Source: ${LOG_TEMP}/${TASKNAME}.*"
    echo "Destination: ${XRDWURL}/${XRDWBASE}/${LOG_DIR}"
    if [ -n ${XRDWURL} ] ; then
      echo "Creating directory: xrdfs ${XRDWURL} mkdir -p ${XRDWBASE}/${LOG_DIR}"
      xrdfs ${XRDWURL} mkdir -p ${XRDWBASE}/${LOG_DIR} || echo "ERROR: Cannot create log directory on xrootd server"
    fi
    echo "Running: xrdcp --debug 2 --force --recursive ${LOG_TEMP}/${TASKNAME}.* ${XRDWURL}/${XRDWBASE}/${LOG_DIR}"
    xrdcp --debug 2 --force --recursive ${LOG_TEMP}/${TASKNAME}.* ${XRDWURL}/${XRDWBASE}/${LOG_DIR} || echo "ERROR: xrdcp failed with exit code $?"
    echo "=== DEBUG: LOG copy attempt completed ==="
  fi
fi

if [ "${COPYFULL:-false}" == "true" ] ; then
  # Validate ROOT file before transfer
  echo "=== Validating FULL ROOT file before transfer ==="
  python $SCRIPT_DIR/validate_rootfile.py "${FULL_TEMP}/${TASKNAME}.edm4hep.root"
  if [ $? -ne 0 ]; then
    echo "ERROR: FULL ROOT file validation failed. Skipping transfer."
    exit 65
  fi
  echo "FULL ROOT file validation passed."

  if [ "${USERUCIO:-false}" == "true" ] ; then
    python $SCRIPT_DIR/register_to_rucio.py -f "${FULL_TEMP}/${TASKNAME}.edm4hep.root" -d "/${FULL_DIR}/${TASKNAME}.edm4hep.root" -s epic -r ${OUT_RSE:-EIC-XRD} --metadata-json "${METADATA_JSON}" || { echo "ERROR: Rucio registration failed for FULL file."; exit 78; }
  else
    # Token for write authentication
    echo "=== DEBUG: Attempting to copy FULL files to xrootd ==="
    echo "BEARER_TOKEN file location: ${_CONDOR_CREDS:-.}/eic.use"
    if [ -f "${_CONDOR_CREDS:-.}/eic.use" ]; then
      export BEARER_TOKEN=$(cat ${_CONDOR_CREDS:-.}/eic.use)
      echo "BEARER_TOKEN loaded successfully"
    else
      echo "WARNING: BEARER_TOKEN file not found at ${_CONDOR_CREDS:-.}/eic.use"
      if [ -f "x509_user_proxy" ]; then
        echo "Found x509_user_proxy, setting X509_USER_PROXY"
        export X509_USER_PROXY="x509_user_proxy"
      fi
    fi
    echo "Source: ${FULL_TEMP}/${TASKNAME}.edm4hep.root"
    echo "Destination: ${XRDWURL}/${XRDWBASE}/${FULL_DIR}"
    if [ -n ${XRDWURL} ] ; then
      echo "Creating directory: xrdfs ${XRDWURL} mkdir -p ${XRDWBASE}/${FULL_DIR}"
      xrdfs ${XRDWURL} mkdir -p ${XRDWBASE}/${FULL_DIR} || echo "ERROR: Cannot create simulation directory on xrootd server"
    fi
    echo "Running: xrdcp --debug 2 --force --recursive ${FULL_TEMP}/${TASKNAME}.edm4hep.root ${XRDWURL}/${XRDWBASE}/${FULL_DIR}"
    xrdcp --debug 2 --force --recursive ${FULL_TEMP}/${TASKNAME}.edm4hep.root ${XRDWURL}/${XRDWBASE}/${FULL_DIR} || echo "ERROR: xrdcp failed with exit code $?"
    echo "=== DEBUG: FULL copy attempt completed ==="
  fi
fi

if [ "${COPYRECO:-false}" == "true" ] ; then
  # Validate ROOT file before transfer
  echo "=== Validating RECO ROOT file before transfer ==="
  python $SCRIPT_DIR/validate_rootfile.py "${RECO_TEMP}/${TASKNAME}.eicrecon.edm4eic.root"
  if [ $? -ne 0 ]; then
    echo "ERROR: RECO ROOT file validation failed. Skipping transfer."
    exit 65
  fi
  echo "RECO ROOT file validation passed."

  if [ "${USERUCIO:-false}" == "true" ] ; then
    python $SCRIPT_DIR/register_to_rucio.py -f "${RECO_TEMP}/${TASKNAME}.eicrecon.edm4eic.root" -d "/${RECO_DIR}/${TASKNAME}.eicrecon.edm4eic.root" -s epic -r ${OUT_RSE:-EIC-XRD} --metadata-json "${METADATA_JSON}" || { echo "ERROR: Rucio registration failed for RECO file."; exit 78; }
  else
    # Token for write authentication
    echo "=== DEBUG: Attempting to copy RECO files to xrootd ==="
    echo "BEARER_TOKEN file location: ${_CONDOR_CREDS:-.}/eic.use"
    if [ -f "${_CONDOR_CREDS:-.}/eic.use" ]; then
      export BEARER_TOKEN=$(cat ${_CONDOR_CREDS:-.}/eic.use)
      echo "BEARER_TOKEN loaded successfully"
    else
      echo "WARNING: BEARER_TOKEN file not found at ${_CONDOR_CREDS:-.}/eic.use"
      if [ -f "x509_user_proxy" ]; then
        echo "Found x509_user_proxy, setting X509_USER_PROXY"
        export X509_USER_PROXY="x509_user_proxy"
      fi
    fi
    echo "Source: ${RECO_TEMP}/${TASKNAME}*.edm4eic.root"
    echo "Destination: ${XRDWURL}/${XRDWBASE}/${RECO_DIR}"
    if [ -n ${XRDWURL} ] ; then
      echo "Creating directory: xrdfs ${XRDWURL} mkdir -p ${XRDWBASE}/${RECO_DIR}"
      xrdfs ${XRDWURL} mkdir -p ${XRDWBASE}/${RECO_DIR} || echo "ERROR: Cannot create reconstruction directory on xrootd server"
    fi
    echo "Running: xrdcp --debug 2 --force --recursive ${RECO_TEMP}/${TASKNAME}*.edm4eic.root ${XRDWURL}/${XRDWBASE}/${RECO_DIR}"
    xrdcp --debug 2 --force --recursive ${RECO_TEMP}/${TASKNAME}*.edm4eic.root ${XRDWURL}/${XRDWBASE}/${RECO_DIR} || echo "ERROR: xrdcp failed with exit code $?"
    echo "=== DEBUG: RECO copy attempt completed ==="
  fi
fi

# closeout
date
find ${TMPDIR}
du -sh ${TMPDIR}
