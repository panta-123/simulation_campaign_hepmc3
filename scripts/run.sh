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
MINIOS3="S3rw/eictest/ATHENA"

# Input file parsing
BASENAME=$(basename ${INPUT_FILE} .hepmc)
INPUT_DIR=$(dirname $(realpath --relative-to=${BASEDIR} ${INPUT_FILE}))
INPUT_PREFIX=${INPUT_DIR/\/*/}
TAG=${INPUT_DIR/${INPUT_PREFIX}\//}
mkdir -p  ${BASEDIR}/FULL/${TAG}
FULL_FILE=${BASEDIR}/FULL/${TAG}/${BASENAME}${TASK}.root
FULL_S3RW=${MINIOS3}/FULL/${TAG}/${BASENAME}${TASK}.root
mkdir -p  ${BASEDIR}/GEOM/${TAG}
GEOM_ROOT=${BASEDIR}/GEOM/${TAG}/${BASENAME}${TASK}.geom
mkdir -p  ${BASEDIR}/RECO/${TAG}
RECO_FILE=${BASEDIR}/RECO/${TAG}/${BASENAME}${TASK}.root
RECO_S3RW=${MINIOS3}/RECO/${TAG}/${BASENAME}${TASK}.root

# Detector description
COMPACT_FILE=/opt/detector/share/athena/athena.xml

# Check for existing full simulation on local node
if [ ! -f ${FULL_FILE} -o ! -d ${GEOM_ROOT} ] ; then
  # Load container environment
  source /opt/detector/setup.sh

  # Run simulation
  /usr/bin/time -v \
    npsim \
    --runType batch \
    --printLevel WARNING \
    --skipNEvents ${SKIP_N_EVENTS} \
    --numberOfEvents ${EVENTS_PER_TASK} \
    --part.minimalKineticEnergy 1*TeV \
    --compactFile ${COMPACT_FILE} \
    --inputFiles ${INPUT_FILE} \
    --outputFile ${FULL_FILE}
  rootls -t "${FULL_FILE}"

  # Take snapshot of geometry and versions
  mkdir -p ${GEOM_ROOT}
  cp -r /opt/detector/* ${GEOM_ROOT}
  eic-info > ${GEOM_ROOT}/eic-info.txt
  echo "export LD_LIBRARY_PATH=${GEOM_ROOT}/lib:$LD_LIBRARY_PATH" > ${GEOM_ROOT}/setup.sh

  # Data egress if S3RW_ACCESS_KEY and S3RW_SECRET_KEY in environment
  if [ -x /usr/local/bin/mc ] ; then
    if ping -c 1 -w 5 google.com > /dev/null ; then
      if [ -n ${S3RW_ACCESS_KEY} -a -n ${S3RW_SECRET_KEY} ] ; then
        /usr/local/bin/mc -C . config host add S3rw https://dtn01.sdcc.bnl.gov:9000 ${S3RW_ACCESS_KEY} ${S3RW_SECRET_KEY}
        /usr/local/bin/mc -C . cp "${FULL_FILE}" "${FULL_S3RW}"
        /usr/local/bin/mc -C . config host remove S3rw 
      else
        echo "No S3 credentials."
      fi
    else
      echo "No internet connection."
    fi
  fi
fi

# Load snapshot environment
source ${GEOM_ROOT}/setup.sh

# Run reconstruction
export JUGGLER_SIM_FILE="${FULL_FILE}"
export JUGGLER_REC_FILE="${RECO_FILE}"
export JUGGLER_N_EVENTS=2147483647
export JUGGLER_DETECTOR=athena
export DETECTOR_PATH="${GEOM_ROOT}/share/athena"
/usr/bin/time -v \
xenv -x /usr/local/Juggler.xenv \
  gaudirun.py /opt/benchmarks/reconstruction_benchmarks/benchmarks/full/options/full_reconstruction.py \
    || [ $? -eq 4 ]
# FIXME why $? = 4
rootls -t "${RECO_FILE}"

# Data egress if S3RW_ACCESS_KEY and S3RW_SECRET_KEY in environment
if [ -x /usr/local/bin/mc ] ; then
  if ping -c 1 -w 5 google.com > /dev/null ; then
    if [ -n ${S3RW_ACCESS_KEY} -a -n ${S3RW_SECRET_KEY} ] ; then
      /usr/local/bin/mc -C . config host add S3rw https://dtn01.sdcc.bnl.gov:9000 ${S3RW_ACCESS_KEY} ${S3RW_SECRET_KEY}
      /usr/local/bin/mc -C . cp "${RECO_FILE}" "${RECO_S3RW}"
      /usr/local/bin/mc -C . config host remove S3rw 
    else
      echo "No S3 credentials."
    fi
  else
    echo "No internet connection."
  fi
fi

# closeout
ls -al ${FULL_FILE}
ls -al ${RECO_FILE}
date
