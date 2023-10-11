#!/bin/bash
voms-proxy-info
echo "++ Who am I ?"
whoami
id

echo "++ setup CMSSW"

source /cvmfs/cms.cern.ch/cmsset_default.sh
scramv1 project ${CMSSW_rel}
cd ${CMSSW_rel}
eval `scramv1 runtime -sh`
echo $CMSSW_VERSION
echo $SCRAM_ARCH

cd ..

echo "++ test crab"
echo "++ crab --version"
crab --version

echo "++ crab checkusername"
crab checkusername

echo "++ crab checkwrite --site T2_CH_CERN"
crab checkwrite --site T2_CH_CERN

