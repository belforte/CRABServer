#!/usr/bin/env python

import sys
import os

from WMCore.Configuration import loadConfigurationFile

configFile = sys.argv[1]
config = loadConfigurationFile(os.path.abspath(configFile))

if getattr(config.TaskWorker, 'useHtcV2', None):
    os.environ['useHtcV2'] = 'True'
    print("V2")
else:
    os.environ.pop('useHtcV2', None)
    print("V1")
