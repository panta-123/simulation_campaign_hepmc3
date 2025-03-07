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
XRDRBASE=${XRDRBASE:-"/work/eic2/EPIC"}

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

# List log files
ls -al ${LOG_TEMP}/${TASKNAME}.*

# Data egress to directory

if [ "${COPYLOG:-false}" == "true" ] ; then
  if [ "${USERUCIO:-false}" == "true" ] ; then
    TIME_TAG=$(date --iso-8601=second)
    tar -czvf ${LOG_TEMP}/${TASKNAME}.log.tar.gz \
    ${LOG_TEMP}/${TASKNAME}.npsim.prmon.txt \
    ${LOG_TEMP}/${TASKNAME}.npsim.log \
    ${LOG_TEMP}/${TASKNAME}.eicrecon.prmon.txt \
    ${LOG_TEMP}/${TASKNAME}.eicrecon.log \
    ${LOG_TEMP}/${TASKNAME}.eicrecon.dot
    python $SCRIPT_DIR/register_to_rucio.py \
    -f "${LOG_TEMP}/${TASKNAME}.log.tar.gz" \
    -d "/${LOG_DIR}/${TASKNAME}.${TIME_TAG}.log.tar.gz" \
    -s epic -r EIC-CLOUD-LOG
  else
    # Token for write authentication
    export BEARER_TOKEN=$(cat ${_CONDOR_CREDS:-.}/eic.use)
    if [ -n ${XRDWURL} ] ; then
      xrdfs ${XRDWURL} mkdir -p ${XRDWBASE}/${LOG_DIR} || echo "Cannot write log outputs to xrootd server"
    else
      mkdir -p ${XRDWBASE}/${LOG_DIR} || echo "Cannot write log outputs to xrootd server"
    fi
    xrdcp --force --recursive ${LOG_TEMP}/${TASKNAME}.* ${XRDWURL}/${XRDWBASE}/${LOG_DIR}
  fi
fi

if [ "${COPYFULL:-false}" == "true" ] ; then
  if [ "${USERUCIO:-false}" == "true" ] ; then
    python $SCRIPT_DIR/register_to_rucio.py -f "${FULL_TEMP}/${TASKNAME}.edm4hep.root" -d "/${FULL_DIR}/${TASKNAME}.edm4hep.root" -s epic -r EIC-XRD
  else
    # Token for write authentication
    export BEARER_TOKEN=$(cat ${_CONDOR_CREDS:-.}/eic.use)
    if [ -n ${XRDWURL} ] ; then
      xrdfs ${XRDWURL} mkdir -p ${XRDWBASE}/${FULL_DIR} || echo "Cannot write simulation outputs to xrootd server"
    else
      mkdir -p ${XRDWBASE}/${FULL_DIR} || echo "Cannot write simulation outputs to xrootd server"
    fi
    xrdcp --force --recursive ${FULL_TEMP}/${TASKNAME}.edm4hep.root ${XRDWURL}/${XRDWBASE}/${FULL_DIR} 
  fi
fi

if [ "${COPYRECO:-false}" == "true" ] ; then
  if [ "${USERUCIO:-false}" == "true" ] ; then
    python $SCRIPT_DIR/register_to_rucio.py -f "${RECO_TEMP}/${TASKNAME}.eicrecon.edm4eic.root" -d "/${RECO_DIR}/${TASKNAME}.eicrecon.edm4eic.root" -s epic -r EIC-XRD
  else
    # Token for write authentication
    export BEARER_TOKEN=$(cat ${_CONDOR_CREDS:-.}/eic.use)
    if [ -n ${XRDWURL} ] ; then
      xrdfs ${XRDWURL} mkdir -p ${XRDWBASE}/${RECO_DIR} || echo "Cannot write reconstructed outputs to xrootd server"
    else
      mkdir -p ${XRDWBASE}/${RECO_DIR} || echo "Cannot write reconstructed outputs to xrootd server"
    fi
    xrdcp --force --recursive ${RECO_TEMP}/${TASKNAME}*.edm4eic.root ${XRDWURL}/${XRDWBASE}/${RECO_DIR}
  fi
fi

# closeout
date
find ${TMPDIR}
du -sh ${TMPDIR}
