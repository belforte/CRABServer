#!/bin/bash
voms-proxy-info
echo "++ Who am I ?"
whoami
id

echo "++ setup CMSSW"

source /cvmfs/cms.cern.ch/latest/cmsset_default.sh
scramv1 project ${CMSSW_rel}
cd ${CMSSW_rel}
eval `scramv1 runtime -sh`

cd ..

echo "++ test crab"
crab --version

crab checkusername

crab checkwrite --site T2_CH_CERN

