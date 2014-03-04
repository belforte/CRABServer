
"""
Create a set of files for a DAG submission.

Generates the condor submit files and the master DAG.
"""

import os
import json
import base64
import shutil
import string
import urllib
import commands
import tempfile
import hashlib

import TaskWorker.Actions.TaskAction as TaskAction
import TaskWorker.DataObjects.Result
import TaskWorker.WorkerExceptions

import WMCore.Services.PhEDEx.PhEDEx as PhEDEx
import WMCore.WMSpec.WMTask

import classad

try:
    from WMCore.Services.UserFileCache.UserFileCache import UserFileCache
except ImportError:
    UserFileCache = None

from ApmonIf import ApmonIf

DAG_HEADER = """

NODE_STATUS_FILE node_state 30

# NOTE: a file must be present, but 'noop' makes it not be read.
#FINAL FinalCleanup Job.1.submit NOOP
#SCRIPT PRE FinalCleanup dag_bootstrap.sh FINAL $DAG_STATUS $FAILED_COUNT %(restinstance)s %(resturl)s

"""

DAG_FRAGMENT = """
JOB Job%(count)d Job.%(count)d.submit
SCRIPT PRE  Job%(count)d dag_bootstrap.sh PREJOB $RETRY %(count)d %(taskname)s %(backend)s
SCRIPT POST Job%(count)d dag_bootstrap.sh POSTJOB $JOBID $RETURN $RETRY $MAX_RETRIES %(restinstance)s %(resturl)s %(taskname)s %(count)d %(outputData)s %(sw)s %(asyncDest)s %(tempDest)s %(outputDest)s cmsRun_%(count)d.log.tar.gz %(remoteOutputFiles)s
#PRE_SKIP Job%(count)d 3
RETRY Job%(count)d 10 UNLESS-EXIT 2
VARS Job%(count)d count="%(count)d" runAndLumiMask="%(runAndLumiMask)s" lheInputFiles="%(lheInputFiles)s" firstEvent="%(firstEvent)s" firstLumi="%(firstLumi)s" lastEvent="%(lastEvent)s" firstRun="%(firstRun)s" seeding="%(seeding)s" inputFiles="%(inputFiles)s" +CRAB_localOutputFiles="\\"%(localOutputFiles)s\\"" +CRAB_DataBlock="\\"%(block)s\\"" +CRAB_Destination="\\"%(destination)s\\""
ABORT-DAG-ON Job%(count)d 3

"""

CRAB_HEADERS = \
"""
+CRAB_ReqName = %(requestname)s
+CRAB_Workflow = %(workflow)s
+CRAB_JobType = %(jobtype)s
+CRAB_JobSW = %(jobsw)s
+CRAB_JobArch = %(jobarch)s
+CRAB_InputData = %(inputdata)s
+CRAB_OutputData = %(publishname)s
+CRAB_ISB = %(cacheurl)s
+CRAB_SiteBlacklist = %(siteblacklist)s
+CRAB_SiteWhitelist = %(sitewhitelist)s
+CRAB_AdditionalOutputFiles = %(addoutputfiles)s
+CRAB_EDMOutputFiles = %(edmoutfiles)s
+CRAB_TFileOutputFiles = %(tfileoutfiles)s
+CRAB_SaveLogsFlag = %(savelogsflag)s
+CRAB_UserDN = %(userdn)s
+CRAB_UserHN = %(userhn)s
+CRAB_AsyncDest = %(asyncdest)s
+CRAB_AsyncDestSE = %(asyncdest_se)s
+CRAB_BlacklistT1 = %(blacklistT1)s
+CRAB_StageoutPolicy = %(stageoutpolicy)s
+CRAB_UserRole = %(tm_user_role)s
+CRAB_UserGroup = %(tm_user_group)s
+CRAB_TaskWorker = %(worker_name)s
"""

JOB_SUBMIT = CRAB_HEADERS + \
"""
CRAB_Attempt = %(attempt)d
CRAB_ISB = %(cacheurl_flatten)s
CRAB_AdditionalOutputFiles = %(addoutputfiles_flatten)s
CRAB_JobSW = %(jobsw_flatten)s
CRAB_JobArch = %(jobarch_flatten)s
CRAB_Archive = %(cachefilename_flatten)s
+CRAB_ReqName = %(requestname)s
#CRAB_ReqName = %(requestname_flatten)s
+CRAB_DBSURL = %(dbsurl)s
+CRAB_PublishDBSURL = %(publishdbsurl)s
CRAB_Publish = %(publication)s
CRAB_Id = $(count)
+CRAB_Id = $(count)
+CRAB_Dest = "%(temp_dest)s"
+CRAB_oneEventMode = %(oneEventMode)s
+CRAB_ASOURL = %(ASOURL)s
+TaskType = "Job"
+AccountingGroup = %(userhn)s

+JOBGLIDEIN_CMSSite = "$$([ifThenElse(GLIDEIN_CMSSite is undefined, \\"Unknown\\", GLIDEIN_CMSSite)])"
job_ad_information_attrs = MATCH_EXP_JOBGLIDEIN_CMSSite, JOBGLIDEIN_CMSSite, RemoteSysCpu, RemoteUserCpu

universe = vanilla
Executable = gWMS-CMSRunAnalysis.sh
Output = job_out.$(CRAB_Id)
Error = job_err.$(CRAB_Id)
Log = job_log
# args changed...

Arguments = "-a $(CRAB_Archive) --sourceURL=$(CRAB_ISB) --jobNumber=$(CRAB_Id) --cmsswVersion=$(CRAB_JobSW) --scramArch=$(CRAB_JobArch) '--inputFile=$(inputFiles)' '--runAndLumis=$(runAndLumiMask)' --lheInputFiles=$(lheInputFiles) --firstEvent=$(firstEvent) --firstLumi=$(firstLumi) --lastEvent=$(lastEvent) --firstRun=$(firstRun) --seeding=$(seeding) -o $(CRAB_AdditionalOutputFiles)"

transfer_input_files = CMSRunAnalysis.sh, cmscp.py%(additional_input_file)s
transfer_output_files = jobReport.json.$(count)
# TODO: fold this into the config file instead of hardcoding things.
Environment = SCRAM_ARCH=$(CRAB_JobArch);%(additional_environment_options)s
should_transfer_files = YES
#x509userproxy = %(x509up_file)s
use_x509userproxy = true
# TODO: Uncomment this when we get out of testing mode
Requirements = ((target.IS_GLIDEIN =!= TRUE) || (target.GLIDEIN_CMSSite =!= UNDEFINED)) %(opsys_req)s
periodic_release = (HoldReasonCode == 28) || (HoldReasonCode == 30) || (HoldReasonCode == 13) || (HoldReasonCode == 6)
# Remove if we've been in the 'held' status for more than 7 minutes, OR
# We are running AND
#  Over memory use OR
#  Over wall clock limit
periodic_remove = ((JobStatus =?= 5) && (time() - EnteredCurrentStatus > 7*60)) || \
                  ((JobStatus =?= 2) && ( \
                     (MemoryUsage > RequestMemory) || \
                     (MaxWallTimeMins*60 < time() - EnteredCurrentStatus) \
                  ))
+PeriodicRemoveReason = ifThenElse(MemoryUsage > RequestMemory, "Removed due to memory use", \
                          ifThenElse(MaxWallTimeMins*60 < time() - EnteredCurrentStatus, "Removed due to wall clock limit", \
                            "Removed due to job being held"))
queue
"""

SPLIT_ARG_MAP = { "LumiBased" : "lumis_per_job",
                  "EventBased" : "events_per_job",
                  "FileBased" : "files_per_job",}


def getCreateTimestamp(taskname):
    return "_".join(taskname.split("_")[:2])


def makeLFNPrefixes(task):
    if task['tm_input_dataset']:
        primaryds = task['tm_input_dataset'].split('/')[1]
    else:
        # For MC
        primaryds = task['tm_publish_name'].rsplit('-', 1)[0]
    hash_input = task['tm_user_dn']
    if 'tm_user_group' in task and task['tm_user_group']:
        hash_input += "," + task['tm_user_group']
    if 'tm_user_role' in task and task['tm_user_role']:
        hash_input += "," + task['tm_user_role']
    hash = hashlib.sha1(hash_input).hexdigest()
    user = task['tm_username']
    tmp_user = "%s.%s" % (user, hash)
    publish_info = task['tm_publish_name'].rsplit('-', 1)
    timestamp = getCreateTimestamp(task['tm_taskname'])
    lfn_prefix = task.get('tm_arguments', {}).get('lfnprefix', '')
    temp_dest = os.path.join("/store/temp/user", tmp_user, lfn_prefix, primaryds, publish_info[0], timestamp)
    dest = os.path.join("/store/user", user, lfn_prefix, primaryds, publish_info[0], timestamp)
    return temp_dest, dest


def transform_strings(input):
    """
    Converts the arguments in the input dictionary to the arguments necessary
    for the job submit file string.
    """
    info = {}
    for var in 'workflow', 'jobtype', 'jobsw', 'jobarch', 'inputdata', 'splitalgo', 'algoargs', \
           'cachefilename', 'cacheurl', 'userhn', 'publishname', 'asyncdest', 'dbsurl', 'publishdbsurl', \
           'userdn', 'requestname', 'publication', 'oneEventMode', 'tm_user_vo', 'tm_user_role', 'tm_user_group', \
           'tm_maxmemory', 'tm_numcores', 'tm_maxjobruntime', 'tm_priority', 'ASOURL', 'asyncdest_se', "stageoutpolicy", \
           'taskType', 'maxpost', 'worker_name':
        val = input.get(var, None)
        if val == None:
            info[var] = 'undefined'
        else:
            info[var] = json.dumps(val)

    for var in 'savelogsflag', 'blacklistT1':
        info[var] = int(input[var])

    for var in 'siteblacklist', 'sitewhitelist', 'addoutputfiles', \
           'tfileoutfiles', 'edmoutfiles':
        val = input[var]
        if val == None:
            info[var] = "{}"
        else:
            info[var] = "{" + json.dumps(val)[1:-1] + "}"

    #TODO: We don't handle user-specified lumi masks correctly.
    info['lumimask'] = '"' + json.dumps(WMCore.WMSpec.WMTask.buildLumiMask(input['runs'], input['lumis'])).replace(r'"', r'\"') + '"'

    splitArgName = SPLIT_ARG_MAP[input['splitalgo']]
    info['algoargs'] = '"' + json.dumps({'halt_job_on_file_boundaries': False, 'splitOnRun': False, splitArgName : input['algoargs']}).replace('"', r'\"') + '"'
    info['attempt'] = 0

    for var in ["cacheurl", "jobsw", "jobarch", "cachefilename", "asyncdest", "requestname"]:
        info[var+"_flatten"] = input[var]

    # TODO: PanDA wrapper wants some sort of dictionary.
    info["addoutputfiles_flatten"] = '{}'

    temp_dest, dest = makeLFNPrefixes(input)
    info["temp_dest"] = temp_dest
    info["output_dest"] = dest
    info['x509up_file'] = os.path.split(input['user_proxy'])[-1]
    info['user_proxy'] = input['user_proxy']
    info['scratch'] = input['scratch']

    return info


def getLocation(default_name, checkout_location):
    loc = default_name
    if not os.path.exists(loc):
        if 'CRABTASKWORKER_ROOT' in os.environ:
            for path in ['xdata','data']:
                fname = os.path.join(os.environ['CRABTASKWORKER_ROOT'], path, loc)
                if os.path.exists(fname):
                    return fname
        if 'CRAB3_CHECKOUT' not in os.environ:
            raise Exception("Unable to locate %s" % loc)
        loc = os.path.join(os.environ['CRAB3_CHECKOUT'], checkout_location, loc)
    loc = os.path.abspath(loc)
    return loc


class DagmanCreator(TaskAction.TaskAction):
    """
    Given a task definition, create the corresponding DAG files for submission
    into HTCondor
    """


    def __init__(self, *args, **kwargs):
        TaskAction.TaskAction.__init__(self, *args, **kwargs)
        self.phedex = PhEDEx.PhEDEx()


    def buildDashboardInfo(self):

        taskType = getattr(self.config.TaskWorker, 'dashboardTaskType', 'analysis')

        params = {'tool': 'crab3',
                  'SubmissionType':'direct',
                  'JSToolVersion': '3.3.0',
                  'tool_ui': os.environ.get('HOSTNAME',''),
                  'scheduler': 'GLIDEIN',
                  'GridName': self.task['tm_user_dn'],
                  'ApplicationVersion': self.task['tm_job_sw'],
                  'taskType': taskType,
                  'vo': 'cms',
                  'CMSUser': self.task['tm_username'],
                  'user': self.task['tm_username'],
                  'taskId': self.task['tm_taskname'],
                  'datasetFull': self.task['tm_input_dataset'],
                  'resubmitter': self.task['tm_username'],
                  'exe': 'cmsRun' }
        return params


    def sendDashboardTask(self):
        apmon = ApmonIf()
        params = self.buildDashboardInfo()
        params_copy = dict(params)
        params_copy['jobId'] = 'TaskMeta'
        self.logger.debug("Dashboard task info: %s" % str(params_copy))
        apmon.sendToML(params_copy)
        apmon.free()
        return params


    def resolvePFNs(self, dest_site, dest_dir, filenames):
        """
        Given a list of filenames, a directory, destination, and the phedex
        object, resolve these to PFNs.
        """
        lfns = [os.path.join(dest_dir, filename) for filename in filenames]
        dest_sites_ = [dest_site]
        if dest_site.startswith("T1_"):
            dest_sites_.append(dest_site + "_Buffer")
            dest_sites_.append(dest_site + "_Disk")
        pfn_info = self.phedex.getPFN(nodes=dest_sites_, lfns=lfns)
        results = []
        for lfn in lfns:
            found_lfn = False
            for suffix in ["", "_Disk", "_Buffer"]:
                if (dest_site + suffix, lfn) in pfn_info:
                    results.append(pfn_info[dest_site + suffix, lfn]) 
                    found_lfn = True
                    break
            if not found_lfn:
                print "Unable to map LFN %s at site %s" % (lfn, dest_site)
        return results


    def makeJobSubmit(self, task):
        """
        Create the submit file.  This is reused by all jobs in the task; differences
        between the jobs are taken care of in the makeSpecs.
        """

        if os.path.exists("Job.submit"):
            return
        # From here on out, we convert from tm_* names to the DataWorkflow names
        info = dict(task)
        info['workflow'] = task['tm_taskname']
        info['jobtype'] = 'analysis'
        info['jobsw'] = info['tm_job_sw']
        info['jobarch'] = info['tm_job_arch']
        info['inputdata'] = info['tm_input_dataset']
        info['splitalgo'] = info['tm_split_algo']
        info['algoargs'] = info['tm_split_args']
        info['cachefilename'] = info['tm_user_sandbox']
        info['cacheurl'] = info['tm_cache_url']
        info['userhn'] = info['tm_username']
        info['publishname'] = info['tm_publish_name']
        info['asyncdest'] = info['tm_asyncdest']
        info['asyncdest_se'] = self.phedex.getNodeSE(info['tm_asyncdest'])
        info['dbsurl'] = info['tm_dbs_url']
        info['publishdbsurl'] = info['tm_publish_dbs_url']
        info['publication'] = info['tm_publication']
        info['userdn'] = info['tm_user_dn']
        info['requestname'] = string.replace(task['tm_taskname'], '"', '')
        info['savelogsflag'] = 0
        info['blacklistT1'] = 0
        info['siteblacklist'] = task['tm_site_blacklist']
        info['sitewhitelist'] = task['tm_site_whitelist']
        info['addoutputfiles'] = task['tm_outfiles']
        info['tfileoutfiles'] = task['tm_tfile_outfiles']
        info['edmoutfiles'] = task['tm_edm_outfiles']
        info['oneEventMode'] = 1 if task.get('tm_arguments', {}).get('oneEventMode', 'F') == 'T' else 0
        info['ASOURL'] = task.get('tm_arguments', {}).get('ASOURL', '')
        info['taskType'] = getattr(self.config.TaskWorker, 'dashboardTaskType', 'analysis')
        info['worker_name'] = getattr(self.config.TaskWorker, 'name', 'unknown')

        # TODO: pass through these correctly.
        info['runs'] = []
        info['lumis'] = []
        info = transform_strings(info)
        info['saveoutput'] = True if task.get('tm_arguments', {}).get('saveoutput', 'T') == 'T' else False
        info['faillimit'] = task.get('tm_arguments', {}).get('faillimit', 10)
        if info['jobarch_flatten'].startswith("slc6_"):
            info['opsys_req'] = '&& (GLIDEIN_REQUIRED_OS=?="rhel6" || OpSysMajorVer =?= 6)'
        else:
            info['opsys_req'] = ''

        info.setdefault("additional_environment_options", '')
        info.setdefault("additional_input_file", "")
        if os.path.exists("CMSRunAnalysis.tar.gz"):
            info['additional_environment_options'] += 'CRAB_RUNTIME_TARBALL=local'
            info['additional_input_file'] += ", CMSRunAnalysis.tar.gz"
        else:
            info['additional_environment_options'] += 'CRAB_RUNTIME_TARBALL=http://hcc-briantest.unl.edu/CMSRunAnalysis-3.3.0-pre1.tar.gz'
        if os.path.exists("TaskManagerRun.tar.gz"):
            info['additional_environment_options'] += ';CRAB_TASKMANAGER_TARBALL=local'
        else:
            info['additional_environment_options'] += ';CRAB_TASKMANAGER_TARBALL=http://hcc-briantest.unl.edu/TaskManagerRun-3.3.0-pre1.tar.gz'
        if os.path.exists("sandbox.tar.gz"):
            info['additional_input_file'] += ", sandbox.tar.gz"
        with open("Job.submit", "w") as fd:
            fd.write(JOB_SUBMIT % info)

        return info


    def makeSpecs(self, task, sitead, jobgroup, block, availablesites, outfiles, startjobid):
        specs = []
        i = startjobid
        temp_dest, dest = makeLFNPrefixes(task)
        for job in jobgroup.getJobs():
            inputFiles = json.dumps([inputfile['lfn'] for inputfile in job['input_files']]).replace('"', r'\"\"')
            runAndLumiMask = json.dumps(job['mask']['runAndLumis']).replace('"', r'\"\"')
            firstEvent = str(job['mask']['FirstEvent'])
            lastEvent = str(job['mask']['LastEvent'])
            firstLumi = str(job['mask']['FirstLumi'])
            firstRun = str(job['mask']['FirstRun'])
            i += 1
            sitead['Job%d' % i] = list(availablesites)
            remoteOutputFiles = []
            localOutputFiles = []
            for origFile in outfiles:
                info = origFile.rsplit(".", 1)
                if len(info) == 2:
                    fileName = "%s_%d.%s" % (info[0], i, info[1])
                else:
                    fileName = "%s_%d" % (origFile, i)
                remoteOutputFiles.append("%s" % fileName)
                localOutputFiles.append("%s=%s" % (origFile, fileName))
            remoteOutputFilesStr = " ".join(remoteOutputFiles)
            localOutputFiles = ", ".join(localOutputFiles)
            if task['tm_input_dataset']:
                primaryds = task['tm_input_dataset'].split('/')[1]
            else:
                # For MC
                primaryds = task['tm_publish_name'].rsplit('-', 1)[0]
            counter = "%04d" % (i / 1000)
            tempDest = os.path.join(temp_dest, counter)
            directDest = os.path.join(dest, counter)
            pfns = self.resolvePFNs(task['tm_asyncdest'], directDest, ["log/cmsRun_%d.log.tar.gz" % i] + remoteOutputFiles)
            pfns = ", ".join(pfns)
            specs.append({'count': i, 'runAndLumiMask': runAndLumiMask, 'inputFiles': inputFiles,
                          'remoteOutputFiles': remoteOutputFilesStr,
                          'localOutputFiles': localOutputFiles, 'asyncDest': task['tm_asyncdest'],
                          'firstEvent' : firstEvent, 'lastEvent' : lastEvent,
                          'firstLumi' : firstLumi, 'firstRun' : firstRun,
                          'seeding' : 'AutomaticSeeding', 'lheInputFiles' : None,
                          'sw': task['tm_job_sw'], 'taskname': task['tm_taskname'],
                          'outputData': task['tm_publish_name'],
                          'tempDest': tempDest,
                          'outputDest': os.path.join(dest, counter),
                          'restinstance': task['restinstance'], 'resturl': task['resturl'],
                          'block': block, 'destination': pfns,
                          'backend': os.environ.get('HOSTNAME','')})

            self.logger.debug(specs[-1])
        return specs, i


    def createSubdag(self, splitter_result, **kwargs):

        startjobid = 0
        specs = []

        if hasattr(self.config.TaskWorker, 'stageoutPolicy'):
            kwargs['task']['stageoutpolicy'] = ",".join(self.config.TaskWorker.stageoutPolicy)
        else:
            kwargs['task']['stageoutpolicy'] = "local,remote"

        info = self.makeJobSubmit(kwargs['task'])

        outfiles = kwargs['task']['tm_outfiles'] + kwargs['task']['tm_tfile_outfiles'] + kwargs['task']['tm_edm_outfiles']

        os.chmod("CMSRunAnalysis.sh", 0755)

        server_data = []

        # This config setting acts as a global black / white list
        global_whitelist = set()
        global_blacklist = set()
        if hasattr(self.config.Sites, 'available'):
            global_whitelist = set(self.config.Sites.available)
        if hasattr(self.config.Sites, 'banned'):
            global_blacklist = set(self.config.Sites.banned)

        sitead = classad.ClassAd()
        for jobgroup in splitter_result:
            jobs = jobgroup.getJobs()

            whitelist = set(kwargs['task']['tm_site_whitelist'])

            if not jobs:
                possiblesites = []
            elif info.get('tm_arguments', {}).get('ignorelocality', 'F') == 'T':
                possiblesites = global_whitelist | whitelist
            else:
                possiblesites = jobs[0]['input_files'][0]['locations']
            block = jobs[0]['input_files'][0]['block']
            self.logger.debug("Block name: %s" % block)
            self.logger.debug("Possible sites: %s" % possiblesites)

            # Apply globals
            availablesites = set(possiblesites) - global_blacklist
            if global_whitelist:
                availablesites &= global_whitelist

            if not availablesites:
                msg = "No site available for submission of task %s" % (kwargs['task']['tm_taskname'])
                raise TaskWorker.WorkerExceptions.NoAvailableSite(msg)

            # NOTE: User can still shoot themselves in the foot with the resubmit blacklist
            # However, this is the last chance we have to warn the users about an impossible task at submit time.
            blacklist = set(kwargs['task']['tm_site_blacklist'])
            available = set(availablesites)
            if whitelist:
                available &= whitelist
                if not available:
                    msg = "You put (%s) in the site whitelist, but your task %s can only run in (%s)" % (", ".join(whitelist), kwargs['task']['tm_taskname'], ", ".join(availablesites))
                    raise TaskWorker.WorkerExceptions.NoAvailableSite(msg)

            available -= (blacklist-whitelist)
            if not available:
                msg = "You put (%s) in the site blacklist, but your task %s can only run in (%s)" % (", ".join(blacklist), kwargs['task']['tm_taskname'], ", ".join(availablesites))
                raise TaskWorker.WorkerExceptions.NoAvailableSite(msg)

            availablesites = [str(i) for i in availablesites]
            self.logger.info("Resulting available sites: %s" % ", ".join(availablesites))

            jobgroupspecs, startjobid = self.makeSpecs(kwargs['task'], sitead, jobgroup, block, availablesites, outfiles, startjobid)
            specs += jobgroupspecs

        dag = DAG_HEADER % {'restinstance': kwargs['task']['restinstance'], 'resturl': self.resturl}
        for spec in specs:
            dag += DAG_FRAGMENT % spec

        with open("RunJobs.dag", "w") as fd:
            fd.write(dag)

        with open("site.ad", "w") as fd:
            fd.write(str(sitead))

        task_name = kwargs['task'].get('CRAB_ReqName', kwargs['task'].get('tm_taskname', ''))
        userdn = kwargs['task'].get('CRAB_UserDN', kwargs['task'].get('tm_user_dn', ''))

        info["jobcount"] = len(specs)
        maxpost = getattr(self.config.TaskWorker, 'maxPost', 20)
        if maxpost == -1:
            maxpost = info['jobcount']
        elif maxpost == 0:
            maxpost = int(max(20, info['jobcount']*.1))
        info['maxpost'] = maxpost

        # Info for ML:
        ml_info = info.setdefault('apmon', [])
        for idx in range(1, info['jobcount']+1):
            taskid = kwargs['task']['tm_taskname'].replace("_", ":")
            jinfo = {'jobId': ("%d_https://glidein.cern.ch/%d/%s_0" % (idx, idx, taskid)),
                     'sid': "https://glidein.cern.ch/%d/%s" % (idx, taskid),
                     'broker': os.environ.get('HOSTNAME',''),
                     'bossId': str(idx),
                     'TargetSE': ("%d_Selected_SE" % len(availablesites)),
                     'localId' : '',
                     'StatusValue' : 'pending',
                    }
            ml_info.append(jinfo)

        # When running in standalone mode, we want to record the number of jobs in the task
        if ('CRAB_ReqName' in kwargs['task']) and ('CRAB_UserDN' in kwargs['task']):
            const = 'TaskType =?= \"ROOT\" && CRAB_ReqName =?= "%s" && CRAB_UserDN =?= "%s"' % (task_name, userdn)
            cmd = "condor_qedit -const '%s' CRAB_JobCount %d" % (const, len(specs))
            self.logger.debug("+ %s" % cmd)
            status, output = commands.getstatusoutput(cmd)
            if status:
                self.logger.error(output)
                self.logger.error("Failed to record the number of jobs.")
                return 1

        return info


    def executeInternal(self, *args, **kw):

        cwd = None
        if hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'scratchDir'):
            temp_dir = tempfile.mkdtemp(prefix='_' + kw['task']['tm_taskname'], dir=self.config.TaskWorker.scratchDir)

            # FIXME: In PanDA, we provided the executable as a URL.
            # So, the filename becomes http:// -- and doesn't really work.  Hardcoding the analysis wrapper.
            #transform_location = getLocation(kw['task']['tm_transformation'], 'CAFUtilities/src/python/transformation/CMSRunAnalysis/')
            transform_location = getLocation('CMSRunAnalysis.sh', 'CRABServer/scripts/')
            cmscp_location = getLocation('cmscp.py', 'CRABServer/scripts/')
            gwms_location = getLocation('gWMS-CMSRunAnalysis.sh', 'CRABServer/scripts/')
            dag_bootstrap_location = getLocation('dag_bootstrap_startup.sh', 'CRABServer/scripts/')
            bootstrap_location = getLocation("dag_bootstrap.sh", "CRABServer/scripts/")
            adjust_location = getLocation("AdjustSites.py", "CRABServer/scripts/")

            cwd = os.getcwd()
            os.chdir(temp_dir)
            shutil.copy(transform_location, '.')
            shutil.copy(cmscp_location, '.')
            shutil.copy(gwms_location, '.')
            shutil.copy(dag_bootstrap_location, '.')
            shutil.copy(bootstrap_location, '.')
            shutil.copy(adjust_location, '.')

            # Bootstrap the ISB if we are using UFC
            if UserFileCache and (kw['task']['tm_cache_url'] == 'https://cmsweb.cern.ch/crabcache/file'):
                ufc = UserFileCache(dict={'cert': kw['task']['user_proxy'], 'key': kw['task']['user_proxy']})
                ufc.download(hashkey=kw['task']['tm_user_sandbox'].split(".")[0], output="sandbox.tar.gz")
                kw['task']['tm_user_sandbox'] = 'sandbox.tar.gz'

            # Bootstrap the runtime if it is available.
            job_runtime = getLocation('CMSRunAnalysis.tar.gz', 'CRABServer/')
            shutil.copy(job_runtime, '.')
            task_runtime = getLocation('TaskManagerRun.tar.gz', 'CRABServer/')
            shutil.copy(task_runtime, '.')

            kw['task']['scratch'] = temp_dir

        kw['task']['restinstance'] = self.server['host']
        kw['task']['resturl'] = self.resturl.replace("/workflowdb", "/filemetadata")
        self.task = kw['task']
        params = self.sendDashboardTask()

        try:
            info = self.createSubdag(*args, **kw)
        finally:
            if cwd:
                os.chdir(cwd)

        return TaskWorker.DataObjects.Result.Result(task=kw['task'], result=(temp_dir, info, params))

    def execute(self, *args, **kw):
        try:
            return self.executeInternal(*args, **kw)
        except Exception, e:
            configreq = {'workflow': kw['task']['tm_taskname'],
                             'substatus': "FAILED",
                             'subjobdef': -1,
                             'subuser': kw['task']['tm_user_dn'],
                             'subfailure': base64.b64encode(str(e)),}
            self.logger.error("Pushing information centrally %s" %(str(configreq)))
            data = urllib.urlencode(configreq)
            self.server.put(self.resturl, data=data)
            raise

