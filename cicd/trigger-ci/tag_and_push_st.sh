#! /bin/bash
set -euo pipefail

ENV=test2
# if ./env  file exist, source it
[[ -f ./env ]] && source ./env
echo ${ENV}
TAG=pypi-${ENV}-$(date +"%s")
git tag $TAG
git push gitlab $TAG -o ci.variable="SUBMIT_STATUS_TRACKING=true" #Use explicit true