# pylint: disable=pointless-string-statement
import os
import sys
import time
import shutil
import logging

from TaskWorker.Actions.Recurring.BaseRecurringAction import BaseRecurringAction

SECONDS_IN_HOUR = 60 * 60
SECONDS_IN_DAY = 60 * 60 * 24

DAYS = SECONDS_IN_HOUR * 6

class RemovetmpDir(BaseRecurringAction):
    pollingTime = 60*24 #minutes

    def _execute(self, config, task):
        self.logger.info('Checking for dirname older than %s days..', (DAYS/SECONDS_IN_DAY))
        now = time.time()
        for dirpath, dirnames, _ in os.walk(config.TaskWorker.scratchDir):
            for dirname in dirnames:
                timestamp = os.path.getmtime(os.path.join(dirpath, dirname))
                if now - DAYS > timestamp:
                    try:
                        self.logger.info('Removing: %s', os.path.join(dirpath, dirname))
                        shutil.rmtree(os.path.join(dirpath, dirname))
                    except Exception as e:
                        self.logger.exception(e)
                    else:
                        self.logger.info('Directory removed.')

if __name__ == '__main__':
    """ Simple main to execute the action standalone. You just need to set the task worker environment.
        The main is set up to work with the production task worker. If you want to use it on your own
        instance you need to change resthost, resturi, and twconfig.
    """

    twconfig = '/data/srv/TaskManager/current/TaskWorkerConfig.py'

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    from WMCore.Configuration import loadConfigurationFile
    cfg = loadConfigurationFile(twconfig)

    rd = RemovetmpDir(cfg.TaskWorker.logsDir)
    rd._execute(cfg, None)  # pylint: disable=protected-access
