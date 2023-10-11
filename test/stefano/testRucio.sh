#!/bin/bash
voms-proxy-info
echo "++ Who am I ?"
whoami
id
echo "++ source /cvmfs/cms.cern.ch/rucio/setup-py3.sh"
source /cvmfs/cms.cern.ch/rucio/setup-py3.sh

echo "++ force RUCIO_ACCOUNT to cmsbot"
export RUCIO_ACCOUNT=`whoami`
export RUCIO_ACCOUNT=cmsbot
echo "++ RUCIO_ACCOUNT= $RUCION_ACCOUNT"

echo "++ test rucio whoami"
rucio whoami && echo SUCCESS
echo ""
