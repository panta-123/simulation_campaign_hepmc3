#!/bin/bash
set -Euo pipefail
trap 's=$?; echo "$0: Error on line "$LINENO": $BASH_COMMAND"; exit $s' ERR
IFS=$'\n\t'

# Load job environment (mask secrets)
if ls environment*.sh ; then
  grep -v SECRET environment*.sh
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
source /opt/detector/epic-${DETECTOR_VERSION:-nightly}/setup.sh
export DETECTOR_CONFIG=${DETECTOR_CONFIG_REQUESTED:-${DETECTOR_CONFIG:-$DETECTOR}}

# Argument parsing
# - input file
INPUT_FILE=${1}
# - number of events
EVENTS_PER_TASK=${2:-10000}
# - current chunk (zero-based)
if [ ${#} -lt 3 ] ; then
  TASK=""
  SEED=1
  SKIP_N_EVENTS=0
else
  # 10-base input task number to 4-zero-padded task number
  TASK=".${3}"
  SEED=$((10#${3}+1))
  # assumes zero-based task number, can be zero-padded 
  SKIP_N_EVENTS=$((10#${3}*EVENTS_PER_TASK))
fi

# Output location
BASEDIR=${DATADIR:-${PWD}}

# XRD and S3 locations
XRDURL="root://dtn-eic.jlab.org//work/eic2/EPIC"
S3URL="https://eics3.sdcc.bnl.gov:9000"

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
if [[ "${INPUT_FILE}" =~ \.hepmc3\.tree\.root ]] ; then
  EXTENSION="${BASH_REMATCH}"
elif [[ "${INPUT_FILE}" =~ \.hepmc[3]?\.gz ]] ; then
  EXTENSION="${BASH_REMATCH}"
elif [[ "${INPUT_FILE}" =~ \.hepmc[3]? ]] ; then
  EXTENSION="${BASH_REMATCH}"
else
  echo "Error: ${INPUT_FILE} has unknown extension!"
  exit 1
fi
BASENAME=$(basename ${INPUT_FILE} ${EXTENSION})
TASKNAME=${BASENAME}${TASK}
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
INPUT_TEMP=${TMPDIR}/EVGEN/${TAG}
mkdir -p ${INPUT_DIR} ${INPUT_TEMP}
TAG=${DETECTOR_VERSION}/${DETECTOR_CONFIG}/${TAG}

# Output file names
LOG_DIR=${BASEDIR}/LOG/${TAG}
LOG_TEMP=${TMPDIR}/LOG/${TAG}
mkdir -p ${LOG_DIR} ${LOG_TEMP}
#
FULL_DIR=${BASEDIR}/FULL/${TAG}
FULL_TEMP=${TMPDIR}/FULL/${TAG}
mkdir -p ${FULL_DIR} ${FULL_TEMP}
#
RECO_DIR=${BASEDIR}/RECO/${TAG}
RECO_TEMP=${TMPDIR}/RECO/${TAG}
mkdir -p ${RECO_DIR} ${RECO_TEMP}

# Internet connectivity check
if curl --connect-timeout 30 --retry 5 --silent --show-error ${S3URL} > /dev/null ; then
  echo "$(hostname) is online."
  export ONLINE=true
else
  echo "$(hostname) is NOT online."
  if which tracepath ; then
    echo "tracepath -b -p 9000 eics3.sdcc.bnl.gov"
    tracepath -b -p 9000 eics3.sdcc.bnl.gov
  fi
  export ONLINE=
fi

# Move input files to temporary location
if [ -f "${INPUT_FILE}" ] ; then
  # Copy input to temp location
  if [ ! -f "${INPUT_TEMP}" ] ; then
    cp -n ${INPUT_FILE} ${INPUT_TEMP}
  fi

  # Unzip if needed
  if [[ ${EXTENSION} =~ (.hepmc[3]?)\.gz$ ]] ; then
    gunzip ${INPUT_TEMP}/${BASENAME}${EXTENSION}
    EXTENSION=${BASH_REMATCH[1]}
  fi

  # Sanitize hepmc
  if [[ ${EXTENSION} =~ \.hepmc[3]?$ ]] ; then
    cat ${INPUT_TEMP}/${BASENAME}${EXTENSION} | sanitize_hepmc3 > ${INPUT_TEMP}/${BASENAME}${EXTENSION}.new
    mv ${INPUT_TEMP}/${BASENAME}${EXTENSION}.new ${INPUT_TEMP}/${BASENAME}${EXTENSION}
  fi

  # Input file points to temporary location
  ls -al ${INPUT_TEMP}/${BASENAME}${EXTENSION}
  INPUT_FILE=${INPUT_TEMP}/${BASENAME}${EXTENSION}

# If remote hepmc.tree.root file, use xrootd protocol
elif [[ "${EXTENSION}" =~ \.hepmc3\.tree\.root$ ]] ; then
  # Prefix with xrootd protocol and URL
  INPUT_FILE=${XRDURL}/${INPUT_FILE}
fi

# Run simulation
{
  date
  eic-info
  prmon \
    --filename ${LOG_TEMP}/${TASKNAME}.npsim.prmon.txt \
    --json-summary ${LOG_TEMP}/${TASKNAME}.npsim.prmon.json \
    -- \
  npsim \
    --runType batch \
    --random.seed ${SEED:-1} \
    --random.enableEventSeed \
    --printLevel WARNING \
    --skipNEvents ${SKIP_N_EVENTS} \
    --numberOfEvents ${EVENTS_PER_TASK} \
    --part.minimalKineticEnergy 1*TeV \
    --filter.tracker 'edep0' \
    --hepmc3.useHepMC3 ${USEHEPMC3:-true} \
    --compactFile ${DETECTOR_PATH}/${DETECTOR_CONFIG}${EBEAM:+${PBEAM:+_${EBEAM}x${PBEAM}}}.xml \
    --inputFiles ${INPUT_FILE} \
    --outputFile ${FULL_TEMP}/${TASKNAME}.edm4hep.root
  ls -al ${FULL_TEMP}/${TASKNAME}.edm4hep.root
} 2>&1 | grep -v SECRET_KEY | tee ${LOG_TEMP}/${TASKNAME}.npsim.log | tail -n1000

# Data egress to directory
if [ "${COPYFULL:-false}" == "true" ] ; then
  cp ${FULL_TEMP}/${TASKNAME}.edm4hep.root ${FULL_DIR}
  ls -al ${FULL_DIR}/${TASKNAME}.edm4hep.root
fi

# Run eicrecon reconstruction
{
  date
  eic-info
  prmon \
    --filename ${LOG_TEMP}/${TASKNAME}.eicrecon.prmon.txt \
    --json-summary ${LOG_TEMP}/${TASKNAME}.eicrecon.prmon.json \
    -- \
  eicrecon \
    -Ppodio:output_file="${RECO_TEMP}/${TASKNAME}.eicrecon.tree.edm4eic.root" \
    -Pjana:warmup_timeout=0 -Pjana:timeout=0 \
    -Pplugins=janadot \
    "${FULL_TEMP}/${TASKNAME}.edm4hep.root"
  if [ -f jana.dot ] ; then mv jana.dot ${LOG_TEMP}/${TASKNAME}.eicrecon.dot ; fi
  ls -al ${RECO_TEMP}/${TASKNAME}*.eicrecon.tree.edm4eic.root
} 2>&1 | grep -v SECRET_KEY | tee ${LOG_TEMP}/${TASKNAME}.eicrecon.log | tail -n1000

# List log files
ls -al ${LOG_TEMP}/${TASKNAME}.*

# Data egress to directory
if [ "${COPYRECO:-false}" == "true" ] ; then
  mv ${RECO_TEMP}/${TASKNAME}*.edm4eic.root ${RECO_DIR}
  ls -al ${RECO_DIR}/${TASKNAME}*.edm4eic.root
fi
if [ "${COPYLOG:-false}" == "true" ] ; then
  mv ${LOG_TEMP}/${TASKNAME}.* ${LOG_DIR}
  ls -al ${LOG_DIR}/${TASKNAME}.*
fi

# closeout
date
find ${TMPDIR}
du -sh ${TMPDIR}
