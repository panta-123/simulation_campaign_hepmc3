#!/bin/bash
set -Euo pipefail
trap 's=$?; echo "$0: Error on line "$LINENO": $BASH_COMMAND"; exit $s' ERR
IFS=$'\n\t'

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

# Load environment
source /opt/detector/setup.sh

# Detector description
COMPACT_FILE=/opt/detector/share/athena/athena.xml

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
echo -n "export LD_LIBRARY_PATH=${GEOM_ROOT}/lib:$" > ${GEOM_ROOT}/setup.sh
echo "LD_LIBRARY_PATH" >> ${GEOM_ROOT}/setup.sh

# Data egress if config.json in $PWD
if [ -x /usr/local/bin/mc -a -f ./config.json ] ; then
  if ping -c 1 -w 5 google.com > /dev/null ; then
    /usr/local/bin/mc -C ./config.json cp "${FULL_FILE}" "${FULL_S3RW}"
  else
    echo "No internet connection."
  fi
fi

# Run reconstruction
source ${GEOM_ROOT}/setup.sh
export JUGGLER_SIM_FILE="${FULL_FILE}"
export JUGGLER_REC_FILE="${RECO_FILE}"
export JUGGLER_N_EVENTS=2147483647
export JUGGLER_DETECTOR=athena
export DETECTOR_PATH="${GEOM_ROOT}/share/athena"
/usr/bin/time -v \
xenv -x /usr/local/Juggler.xenv \
  gaudirun.py /opt/benchmarks/reconstruction_benchmarks/benchmarks/full/options/full_reconstruction.py
rootls -t "${RECO_FILE}"

# Data egress if config.json in $PWD
if [ -x /usr/local/bin/mc -a -f ./config.json ] ; then
  if ping -c 1 -w 5 google.com > /dev/null ; then
    /usr/local/bin/mc -C ./config.json cp "${RECO_FILE}" "${RECO_S3RW}"
  else
    echo "No internet connection."
  fi
fi

# closeout
ls -al ${FULL_FILE}
ls -al ${RECO_FILE}
date
