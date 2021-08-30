#!/bin/bash
set -Euo pipefail
trap 's=$?; echo "$0: Error on line "$LINENO": $BASH_COMMAND"; exit $s' ERR
IFS=$'\n\t'

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
date
hostname
whoami
pwd
ls -al

# Load container environment (include ${DETECTOR_VERSION})
source /opt/detector/setup.sh
echo "DETECTOR_VERSION=${DETECTOR_VERSION}"

# Argument parsing
# - input file
INPUT_FILE=${1}
# - number of events
EVENTS_PER_TASK=${2:-10000}
# - current chunk
if [ ${#} -lt 3 ] ; then
  TASK=""
  SKIP_N_EVENTS=0
else
  TASK=$(printf ".%04d" ${3})
  SKIP_N_EVENTS=$(((${3}-1)*EVENTS_PER_TASK))
fi

# Output location
BASEDIR=${DATADIR:-${PWD}}

# S3 locations
MC="/usr/local/bin/mc"
S3URL="https://dtn01.sdcc.bnl.gov:9000"
S3RO="S3"
S3RW="S3rw"
S3RODIR="${S3RO}/eictest/ATHENA"
S3RWDIR="${S3RW}/eictest/ATHENA"

# Input file parsing
BASENAME=$(basename ${INPUT_FILE} .hepmc)
INPUT_DIR=$(dirname $(realpath --canonicalize-missing --relative-to=${BASEDIR} ${INPUT_FILE}))/
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
mkdir -p   ${BASEDIR}/EVGEN/${TAG}
INPUT_S3RO=${S3RODIR}/EVGEN/${TAG}/${BASENAME}.hepmc
INPUT_S3RO=${INPUT_S3RO//\/\//\/}
TAG=${DETECTOR_VERSION}/${TAG}

# Output file names
mkdir -p ${BASEDIR}/LOG/${TAG}
LOG_FILE=${BASEDIR}/LOG/${TAG}/${BASENAME}${TASK}.out
LOG_S3RW=${S3RWDIR}/LOG/${TAG}/${BASENAME}${TASK}.out
LOG_S3RW=${LOG_S3RW//\/\//\/}
mkdir -p  ${BASEDIR}/FULL/${TAG}
FULL_FILE=${BASEDIR}/FULL/${TAG}/${BASENAME}${TASK}.root
FULL_S3RW=${S3RWDIR}/FULL/${TAG}/${BASENAME}${TASK}.root
FULL_S3RW=${FULL_S3RW//\/\//\/}
mkdir -p  ${BASEDIR}/RECO/${TAG}
RECO_FILE=${BASEDIR}/RECO/${TAG}/${BASENAME}${TASK}.root
RECO_S3RW=${S3RWDIR}/RECO/${TAG}/${BASENAME}${TASK}.root
RECO_S3RW=${RECO_S3RW//\/\//\/}

# Local temp dir
if [ test -n "${SLURM_TMPDIR:-}" ] ; then
  TMPDIR=${SLURM_TMPDIR}
else
  if [ test -d "/scratch/slurm/${SLURM_JOB_ID:-}" ] ; then
    TMPDIR="/scratch/slurm/${SLURM_JOB_ID:-}"
  else
    TMPDIR=${TMPDIR:-/tmp}/${$}
  fi
fi
mkdir -p ${TMPDIR}/EVGEN/${TAG}/ ${TMPDIR}/FULL/${TAG}/ ${TMPDIR}/RECO/${TAG}/ ${TMPDIR}/LOG/${TAG}/
INPUT_TEMP=${TMPDIR}/EVGEN/${TAG}/${BASENAME}${TASK}.hepmc
FULL_TEMP=${TMPDIR}/FULL/${TAG}/${BASENAME}${TASK}.root
RECO_TEMP=${TMPDIR}/RECO/${TAG}/${BASENAME}${TASK}.root
LOG_TEMP=${TMPDIR}/LOG/${TAG}/${BASENAME}${TASK}.out

# Start logging block
{

# Retrieve input file if S3_ACCESS_KEY and S3_SECRET_KEY in environment
if [ ! -f ${INPUT_FILE} ] ; then
  if [ -x ${MC} ] ; then
    if curl --connect-timeout 5 ${S3URL} > /dev/null ; then
      if [ -n "${S3_ACCESS_KEY:-}" -a -n "${S3_SECRET_KEY:-}" ] ; then
        ${MC} -C . config host add ${S3RO} ${S3URL} ${S3_ACCESS_KEY} ${S3_SECRET_KEY}
        ${MC} -C . cp --disable-multipart "${INPUT_S3RO}" "${INPUT_FILE}"
        ${MC} -C . config host remove ${S3RO}
      else
        echo "No S3 credentials. Provide (readonly) S3 credentials."
        exit
      fi
    else
      echo "No internet connection. Pre-cache input file."
      exit
    fi
  fi
fi

# Run simulation
cp "${INPUT_FILE}" "${INPUT_TEMP}"
/usr/bin/time -v \
  npsim \
  --runType batch \
  --printLevel WARNING \
  --skipNEvents ${SKIP_N_EVENTS} \
  --numberOfEvents ${EVENTS_PER_TASK} \
  --part.minimalKineticEnergy 1*TeV \
  --hepmc3.useHepMC3 ${USEHEPMC3:-true} \
  --compactFile ${DETECTOR_PATH}/${JUGGLER_DETECTOR}.xml \
  --inputFiles "${INPUT_TEMP}" \
  --outputFile "${FULL_TEMP}"
rootls -t "${FULL_TEMP}"
cp "${FULL_TEMP}" "${FULL_FILE}"

# Data egress if S3RW_ACCESS_KEY and S3RW_SECRET_KEY in environment
if [ -x ${MC} ] ; then
  if curl --connect-timeout 5 ${S3URL} > /dev/null ; then
    if [ -n "${S3RW_ACCESS_KEY:-}" -a -n "${S3RW_SECRET_KEY:-}" ] ; then
      ${MC} -C . config host add ${S3RW} ${S3URL} ${S3RW_ACCESS_KEY} ${S3RW_SECRET_KEY}
      ${MC} -C . cp --disable-multipart "${FULL_FILE}" "${FULL_S3RW}"
      ${MC} -C . config host remove ${S3RW}
    else
      echo "No S3 credentials."
    fi
  else
    echo "No internet connection."
  fi
fi

# Get calibrations (e.g. 'acadia-v1.0-alpha' will pull artifacts from 'acadia')
if [ ! -d config ] ; then
  ${RECONSTRUCTION:-/opt/benchmarks/reconstruction_benchmarks}/bin/get_calibrations ${DETECTOR_VERSION/-*/}
fi

# Run reconstruction
export JUGGLER_SIM_FILE="${FULL_TEMP}"
export JUGGLER_REC_FILE="${RECO_TEMP}"
export JUGGLER_N_EVENTS=2147483647
/usr/bin/time -v \
  gaudirun.py ${RECONSTRUCTION:-/opt/benchmarks/reconstruction_benchmarks}/benchmarks/full/options/full_reconstruction.py \
  || [ $? -eq 4 ]
# FIXME why $? = 4
rootls -t "${RECO_TEMP}"
cp "${RECO_TEMP}" "${RECO_FILE}"

} 2>&1 | tee "${LOG_TEMP}"
cp "${LOG_TEMP}" "${LOG_FILE}"

# Data egress if S3RW_ACCESS_KEY and S3RW_SECRET_KEY in environment
if [ -x ${MC} ] ; then
  if curl --connect-timeout 5 ${S3URL} > /dev/null ; then
    if [ -n "${S3RW_ACCESS_KEY:-}" -a -n "${S3RW_SECRET_KEY:-}" ] ; then
      ${MC} -C . config host add ${S3RW} ${S3URL} ${S3RW_ACCESS_KEY} ${S3RW_SECRET_KEY}
      ${MC} -C . cp --disable-multipart "${RECO_FILE}" "${RECO_S3RW}"
      ${MC} -C . cp --disable-multipart "${LOG_FILE}" "${LOG_S3RW}"
      ${MC} -C . config host remove ${S3RW}
    else
      echo "No S3 credentials."
    fi
  else
    echo "No internet connection."
  fi
fi

# closeout
rm -f "${INPUT_TEMP}"
rm -f "${FULL_TEMP}"
rm -f "${RECO_TEMP}"
ls -al "${FULL_FILE}"
ls -al "${RECO_FILE}"
ls -al "${LOG_FILE}"
date
