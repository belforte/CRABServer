#!/bin/bash

unset X509_USER_PROXY
unset X509_USER_CERT
unset X509_USER_KEY

# if PUBLISHER_HOME is already defined, use it
if [ -v PUBLISHER_HOME ]
then
  echo "PUBLISHER_HOME already set to $PUBLISHER_HOME. Will use that"
else
  thisScript=`realpath $0`
  myDir=`dirname ${thisScript}`
  export PUBLISHER_HOME=${myDir}  # where we run the Publisher and where Config is
  echo "Define environment for Publisher in $PUBLISHER_HOME"
fi

source ${PUBLISHER_HOME}/env.sh

rm -f nohup.out

__strip_pythonpath(){
# this function is used to strip the taskworker lines from $PYTHONPATH
# in order for the debug |private calls to be able to add theirs

local strip_reg=".*crabtaskworker.*"
local ppath_init=${PYTHONPATH//:/: }
local ppath_stripped=""

for i in $ppath_init
do
    [[ $i =~ $strip_reg ]] || ppath_stripped="${ppath_stripped}${i}"
done
# echo -e "brfore strip: \n$ppath_init" |sed -e 's/\:/\:\n/g'
# echo -e "after strip: \n$ppath_stripped" |sed -e 's/\:/\:\n/g'
export PYTHONPATH=$ppath_stripped
}

case $1 in
    debug)
        # debug current installation
	python $PUBLISHER_ROOT/lib/python2.7/site-packages/Publisher/SequentialPublisher.py --config $PUBLISHER_HOME/PublisherConfig.py --debug
	;;
    private)
        # run from private repos in /data/user
	__strip_pythonpath
	export PYTHONPATH=/data/user/CRABServer/src/python:/data/user/WMCore/src/python:$PYTHONPATH
	nohup python /data/user/CRABServer/src/python/Publisher/PublisherMaster.py --config $PUBLISHER_HOME/PublisherConfig.py &
	;;
    test)
        # debug private repos in /data/user
        __strip_pythonpath
        export PYTHONPATH=/data/user/CRABServer/src/python:/data/user/WMCore/src/python:$PYTHONPATH
        python -m pdb /data/user/CRABServer/src/python/Publisher/SequentialPublisher.py --config $PUBLISHER_HOME/PublisherConfig.py --debug
	;;
    *)
        # run current installation (production mode)
	nohup python $PUBLISHER_ROOT/lib/python2.7/site-packages/Publisher/PublisherMaster.py --config $PUBLISHER_HOME/PublisherConfig.py &
	;;
esac
