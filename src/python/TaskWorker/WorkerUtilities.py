"""
Common functions to be reused around TW and Publisher
"""

import logging
import functools
import os
import random
import re
import time
import datetime

from http.client import HTTPException
from urllib.parse import urlencode

import requests

from ServerUtilities import truncateError, tempSetLogLevel, SERVICE_INSTANCES
from RESTInteractions import CRABRest
from WMCore.Services.CRIC.CRIC import CRIC
from TaskWorker.WorkerExceptions import ConfigException

globalCachedUserMap = {}
globalCacheExpireTime = 0


def getCrabserver(restConfig=None, agentName='crabtest', logger=None):
    """
    given a configuration object which contains instance, cert and key
    builds a crabserver object. It allows to set agent name so that
    requests by different clients can be separately monitored
    """

    try:
        instance = restConfig.instance
    except AttributeError as exc:
        msg = "No instance provided: need to specify restConfig.instance in the configuration"
        raise ConfigException(msg) from exc

    if instance in SERVICE_INSTANCES:
        logger.info('Will connect to CRAB service: %s', instance)
        restHost = SERVICE_INSTANCES[instance]['restHost']
        dbInstance = SERVICE_INSTANCES[instance]['dbInstance']
    else:
        msg = f"Invalid instance value '{instance}'"
        raise ConfigException(msg)
    if instance == 'other':
        logger.info('Will use restHost and dbInstance from config file')
        try:
            restHost = restConfig.restHost
            dbInstance = restConfig.dbInstance
        except AttributeError as exc:
            msg = "Need to specify restConfig.restHost and dbInstance in the configuration"
            raise ConfigException(msg) from exc

    # Let's increase the server's retries for recoverable errors in the MasterWorker
    # 20 means we'll keep retrying for about 1 hour
    # we wait at 20*NUMRETRY seconds after each try, so retry at: 20s, 60s, 120s ... 20*(n*(n+1))/2
    crabserver = CRABRest(restHost, restConfig.cert, restConfig.key, retry=20,
                               logger=logger, userAgent=agentName)
    crabserver.setDbInstance(dbInstance)

    logger.info('Will connect to CRAB REST via: https://%s/crabserver/%s', restHost, dbInstance)

    return crabserver


def uploadWarning(warning=None, taskname=None, crabserver=None, logger=None):
    """
    Uploads a warning message to the Task DB so that crab status can show it
    :param warning: string: message text
    :param taskname: string: name of the task
    :param crabserver: an instance of CRABRest class
    :param logger: logger
    :return:
    """

    if not crabserver:  # When testing, the server can be None
        logger.warning(warning)
        return

    truncWarning = truncateError(warning)
    configreq = {'subresource': 'addwarning',
                 'workflow': taskname,
                 'warning': truncWarning}
    try:
        crabserver.post(api='task', data=urlencode(configreq))
    except HTTPException as hte:
        logger.error("Error uploading warning: %s", str(hte))
        logger.warning("Cannot add a warning to REST interface. Warning message: %s", warning)


def deleteWarnings(taskname=None, crabserver=None, logger=None):
    """
    deletes all warning messages uploaed for a task
    """
    configreq = {'subresource': 'deletewarnings', 'workflow': taskname}
    try:
        crabserver.post(api='task', data=urlencode(configreq))
    except HTTPException as hte:
        logger.error("Error deleting warnings: %s", str(hte))
        logger.warning("Can not delete warnings from REST interface.")


def safeGet(obj, key, default=None):
    """
    Try dictionary-style access first, otherwise try attribute access.
    """
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def suppressExternalServiceLogging(func):
    """
    Suppresses logging for a function of external service.
    
    Note: Assumes `self.logger` is defined.

    Example:
        @suppressExternalServiceLogging
        def some_external_call(self, ...):
            # External call with suppressed logging
            ...
    """

    @functools.wraps(func)
    def _wrapper(self, *args, **kwargs):
        with tempSetLogLevel(
            logger=safeGet(self, "logger", default=logging.getLogger()),
            level=logging.ERROR,
        ):
            return func(self, *args, **kwargs)

    return _wrapper


class CRICService(CRIC):
    """
    WMCore's CRIC Service with logging suppressed.
    """

    def __init__(self, *args, **kwargs):
        with tempSetLogLevel(logger=kwargs["logger"], level=logging.ERROR):
            super().__init__(*args, **kwargs)

    @suppressExternalServiceLogging
    def _getResult(self, *args, **kwargs):
        """ override super class method to suppress logging """
        return super()._getResult(*args, **kwargs)

    @suppressExternalServiceLogging
    def PNNstoPSNs(self, *args, **kwargs):
        """ maps PhexedNodeNames (i.e. RSE's) to ProcessingSiteNames (i.e. sites) """
        return super().PNNstoPSNs(*args, **kwargs)

class MapUsersToGroups():
    """ prepares a map from users to local groups or high priority status """

    globalCachedUserMap = {}
    globalCacheExpireTime = 0

    def __init__(self, config, logger):
        """
        requires a config.TaskWorker object where cmscert and cmskey are defined
                """
        self.config = config
        self.logger = logger
        self.logger.info("===== MapsUsersToGroups __init__ called")
        self.logger.info("===== globalCacheExpireTime is %s", MapUsersToGroups.globalCacheExpireTime)
        self.logger.info("===== globalCachedUserMap is %s", MapUsersToGroups.globalCachedUserMap)

    def cacheMap(self):
        """
        creates a cachedUserMap dictionary with mapping of user name to local sites
        or high priority groups, format is
        {{'username': {'sites': set(site1, site2,...), 'hiPrio': True/False (boolean)}}, ... }
        e.g.
        {'belforte': {'sites': ('T3_US_FNALLPC'), 'hiPrio': False}},
        """

        self.logger.info("===== caching user map at %s", datetime.datetime.now())
        cache = {}
        # start with mapping from users to local groups
        usersToSites = self.cacheUsersToSites()
        if not usersToSites:
            # caching failed, retry soonish
            MapUsersToGroups.globalCacheExpireTime = time.time() + 60
            return
        for user in usersToSites:
            cache[user] = {}
            cache[user]['sites'] = usersToSites[user]
            cache[user]['hiPrio'] = False

        # then add highprio info
        highPriorityUsers = self.getHighPrioUsersFromCRIC()
        for user in highPriorityUsers:
            if not user in cache:
                cache[user]['sites'] = set()
            cache[user]['hiPrio'] = True

        MapUsersToGroups.globalCachedUserMap = cache
        MapUsersToGroups.globalCacheExpireTime = time.time() + 15*60
        humanTime =  datetime.datetime.fromtimestamp(MapUsersToGroups.globalCacheExpireTime).strftime('%H:%M:%S')
        self.logger.info("===== new cache expire time: %s", humanTime)

    def getSitesForUser(self, user):
        self.logger.info("===== in getSitesForUsers")
        self.logger.info(f"=====  cache expire time is {MapUsersToGroups.globalCacheExpireTime}")
        self.logger.info(f"=====  cached map is {MapUsersToGroups.globalCachedUserMap}")

        if time.time() > MapUsersToGroups.globalCacheExpireTime:
            self.cacheMap()
        if user in MapUsersToGroups.globalCachedUserMap:
            return MapUsersToGroups.globalCachedUserMap[user]['sites']
        else:
            return set()

    def isUserInHighPriorityGroup(self, user):
        """
        returns True/False
        """
        self.logger.info("===== in isUserInHighPriorityGroup")
        self.logger.info(f"=====  cache expire time is {MapUsersToGroups.globalCacheExpireTime}")
        self.logger.info(f"=====  cached map is {MapUsersToGroups.globalCachedUserMap}")
        if time.time() > MapUsersToGroups.globalCacheExpireTime:
            self.cacheMap()
        if user in MapUsersToGroups.globalCachedUserMap:
            return MapUsersToGroups.globalCachedUserMap[user]['hiPrio']
        else:
            return False

    def cacheUsersToSites(self):
        """ Cache the entries in the variuos local-users.txt files
            Those entries are saved in dictionary with this
            format:

            {'username1': set(['T3_IT_Bologna']),
             'username2': set(['T2_US_Nebraska']),
             'username3': set(['T2_ES_CIEMAT', 'T3_IT_Bologna']),
             'userdn1': set(['T2_ES_CIEMAT']),
             'userfqan: set(['T2_ES_CIEMAT', 'T3_IT_Bologna'])
        """
        # adapted from CMSGroupMapper.cache_users originally created by B.Bockelman

        usersToSites = {}
        base_dir = '/cvmfs/cms.cern.ch/SITECONF'
        user_re = re.compile(r'[-_A-Za-z0-9.]+')
        sites = None
        try:
            if os.path.isdir(base_dir):
                sites = os.listdir(base_dir)
        except OSError as ose:
            self.logger.warning("Cannot list SITECONF directories in cvmfs:" + str(ose))
        if not sites:
            return {}
        for siteName in sites:
            if (siteName == 'local'):
                continue
            full_path = os.path.join(base_dir, siteName, 'GlideinConfig', 'local-users.txt')
            if os.path.isfile(full_path):
                try:
                    with open(full_path) as fd:
                        for user in fd:
                            user = user.strip()
                            if user_re.match(user):
                                group_set = usersToSites.setdefault(user, set())
                                group_set.add(siteName)
                except OSError as ose:
                    self.logger.errror("Cannot list SITECONF directories in cvmfs:" + str(ose))
                    raise
        return usersToSites

    @suppressExternalServiceLogging
    def getHighPrioUsersFromCRIC(self):
        """
        get the list of high priority users from CRIC with retries
        arguments:
          cert,key : string : absolute path name for PEM file to use for authentication
          logger : a logging.logger object
        """

        cricGroup = 'CMS_CRAB_HighPrioUsers'
        cricUrl = f"https://cms-cric.cern.ch//api/accounts/group/query/?json&name={cricGroup}"
        cert = self.config.cmscert
        key = self.config.cmskey
        capath = '/etc/grid-security/certificates'

        nRetries = 4
        for i in range(nRetries + 1):
            try:
                # make HTTP GET
                r = requests.get(url=cricUrl, cert=(cert, key), verify=capath, timeout=10)
                # get JSON output and parse it
                highPrioUsers = []
                for user in r.json()[cricGroup]['users']:
                    highPrioUsers.append(user['login'])
            except Exception as ex:  # pylint: disable=broad-except
                if i < nRetries:
                    sleeptime = 20 * (i + 1) + random.randint(-10, 10)
                    msg = f"Sleeping {sleeptime} seconds after HTTP error. Error details:\n{ex}"
                    self.logger.debug(msg)
                    time.sleep(sleeptime)
                else:
                    # this was the last retry
                    msg = "Error when getting the high priority users list from CRIC." \
                          " Will ignore the high priority list and continue normally." \
                          f" Error reason:\n{ex}"
                    self.logger.error(msg)
                    highPrioUsers = []
                    break
            else:
                break
        return highPrioUsers
