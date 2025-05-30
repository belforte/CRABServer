""" handles HTTP queries to crabserver/instance/task?...  """
# pylint: disable=unused-argument
import re
import logging
from ast import literal_eval

from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Validation import validate_str, validate_strlist
from WMCore.REST.Error import InvalidParameter, ExecutionError, NotAcceptable

from CRABInterface.Utilities import conn_handler, getDBinstance
from CRABInterface.RESTExtensions import authz_login_valid, authz_owner_match, authz_operator
from CRABInterface.Regexps import RX_MANYLINES_SHORT, RX_SUBRES_TASK, RX_TASKNAME, RX_STATUS, RX_USERNAME,\
    RX_RUNS, RX_OUT_DATASET, RX_URL, RX_SCHEDD_NAME, RX_RUCIORULE, RX_DATASET, RX_ANYTHING_10K, \
    RX_TASK_COLUMN
from ServerUtilities import getUsernameFromTaskname


class RESTTask(RESTEntity):
    """REST entity to handle interactions between CAFTaskWorker and TaskManager database"""

    @staticmethod
    def globalinit(centralcfg=None):
        RESTTask.centralcfg = centralcfg

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.Task = getDBinstance(config, 'TaskDB', 'Task')
        self.logger = logging.getLogger("CRABLogger.RESTTask")

    def validate(self, apiobj, method, api, param, safe):
        """Validating all the input parameter as enforced by the WMCore.REST module"""
        authz_login_valid()
        if method in ['POST']:
            validate_str('subresource', param, safe, RX_SUBRES_TASK, optional=False)
            validate_str("workflow", param, safe, RX_TASKNAME, optional=True)
            validate_str("warning", param, safe, RX_MANYLINES_SHORT, optional=True)
            validate_str("webdirurl", param, safe, RX_URL, optional=True)
            validate_str("scheddname", param, safe, RX_SCHEDD_NAME, optional=True)
            validate_strlist("outputdatasets", param, safe, RX_OUT_DATASET)
            validate_str("taskstatus", param, safe, RX_STATUS, optional=True)
            validate_str("ddmreqid", param, safe, RX_RUCIORULE, optional=True)
            validate_str("transfercontainer", param, safe, RX_DATASET, optional=True)
            validate_str("transferrule", param, safe, RX_RUCIORULE, optional=True)
            validate_str("publishrule", param, safe, RX_RUCIORULE, optional=True)
            validate_str("column", param, safe, RX_TASK_COLUMN, optional=True)
            validate_str("value", param, safe, RX_ANYTHING_10K, optional=True)
            # Save json string directly to tm_multipub_rule CLOB column.
            validate_str("multipubrulejson", param, safe, RX_ANYTHING_10K, optional=True)
        elif method in ['GET']:
            validate_str('subresource', param, safe, RX_SUBRES_TASK, optional=False)
            validate_str("workflow", param, safe, RX_TASKNAME, optional=True)
            validate_str('taskstatus', param, safe, RX_STATUS, optional=True)
            validate_str('username', param, safe, RX_USERNAME, optional=True)
            validate_str('minutes', param, safe, RX_RUNS, optional=True)
            validate_str("ddmreqid", param, safe, RX_RUCIORULE, optional=True)

    @restcall
    def get(self, subresource, **kwargs):
        """Retrieves the server information, like delegateDN, filecacheurls ...
           :arg str subresource: the specific server information to be accessed;
        """
        return getattr(RESTTask, subresource)(self, **kwargs)

    def allusers(self, **kwargs):
        rows = self.api.query(None, None, self.Task.ALLUSER_sql)
        return rows

    # Stefano - 10 Mar 2025 : AFAICT this is not used anywhere
    def allinfo(self, **kwargs):
        rows = self.api.query(None, None, self.Task.IDAll_sql, taskname=kwargs['workflow'])
        return rows

	#INSERTED BY ERIC SUMMER STUDENT
    def summary(self, **kwargs):
        """ Retrieves the data for list all users"""
        rows = self.api.query(None, None, self.Task.TASKSUMMARY_sql)
        return rows

    # short status summary for the UI MAIN tab
    def status(self, **kwargs):
        """
         The API is (only?) used in the monitor for operator.
           curl -X GET 'https://mmascher-dev6.cern.ch/crabserver/dev/task?subresource=statush&workflow=150224_230633:mmascher_crab_testecmmascher-dev6_3' \
                        -k --key /tmp/x509up_u8440 --cert /tmp/x509up_u8440 -v
        to be used in the CRABServer UI to display a summary of Task status
        moved here from RESTWorkflow to have it together with the "search" below
        after the deprecation of old worfklow/status API which was calling HTCondor inside
        """
        # this retuns a JSON "ready to be displayed" of the form {key:value,...}
        # different from the format of many REST API
        # {'desc':{'columns':[list of column names]}, 'result':[list of column values]}
        # wo we need to repeat the SQL query used in old code in
        # https://github.com/dmwm/CRABServer/blob/5c7777045b33302fb97128ed29d4627141e009ec/src/python/CRABInterface/HTCondorDataWorkflow.py#L170
        # with the two steps:
        # 1. retrieve the values
        # 2. assign them to a name ntuple so to be able to address them

        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")
        taskName =  kwargs['workflow']
        try:
            row = self.api.query(None, None, self.Task.ID_sql, taskname = taskName)  # step 1. get values
            #just one row is picked up by the previous query
            row = self.Task.ID_tuple(*next(row))  # step 2. assign name to values
        except StopIteration as e:
            raise ExecutionError("Impossible to find task %s in the database." % taskName) from e

        # now pick code from old implementation
        # Empty results, only fields for which a non NULL value was retrieved from DB will be filled
        result = {"submissionTime"   : '',
                  "username"         : '',
                  "status"           : '',
                  "DAGstatus"        : '',
                  "command"          : '',
                  "taskFailureMsg"   : '',
                  "taskWarningMsg"   : [],
                  "splitting"        : '',
                  "numJobs"          : '',
                  "schedd"           : '',
                  "taskWorker"       : '',
                  "webdirPath"       : '',
                  "clusterid"        : ''}

        result['submissionTime'] = str(row.start_time)  # convert from datetime object to readable string
        result['status'] = row.task_status
        if row.task_command:
            result['command'] = row.task_command
        if row.task_failure:
            if isinstance(row.task_failure, str):
                result['taskFailureMsg'] = row.task_failure
            else:
                result['taskFailureMsg'] = row.task_failure.read()
        # Need to use literal_eval because task_warnings is a list of strings stored as a CLOB in the DB
        if row.task_warnings:
            taskWarnings = literal_eval(row.task_warnings if isinstance(row.task_warnings, str) else row.task_warnings.read())
            result["taskWarningMsg"] = taskWarnings
        if row.schedd:
            result['schedd'] = row.schedd
        if row.split_algo:
            result['splitting'] = row.split_algo
        if row.numJobs:
            result['numJobs'] = row.numJobs
        if row.twname:
            result['taskWorker'] = row.twname
        if row.user_webdir:
            result['webdirPath'] =  '/'.join(['/home/grid']+row.user_webdir.split('/')[-2:])
        if row.username:
            result['username'] = row.username
        if row.clusterid:
            result['clusterid'] = row.clusterid
        if row.DAGstatus:
            result['DAGstatus'] = row.DAGstatus
        return [result]

    #Quick search api
    def search(self, **kwargs):
        """Retrieves all the columns of a task in the task table (select * from task ...)
           The API is (only?) used in the monitor for operator.
           curl -X GET 'https://mmascher-dev6.cern.ch/crabserver/dev/task?subresource=search&workflow=150224_230633:mmascher_crab_testecmmascher-dev6_3' \
                        -k --key /tmp/x509up_u8440 --cert /tmp/x509up_u8440 -v"""

        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")

        try:
            row = next(self.api.query(None, None, self.Task.QuickSearch_sql, taskname=kwargs["workflow"]))
        except StopIteration as e:
            raise ExecutionError("Impossible to find task %s in the database." % kwargs["workflow"]) from e

        def getval(col):
            """ Some columns in oracle can be CLOB and we need to call read on them.
            """
            # should move this function in ServerUtils and use it when required (e.g.: mysql LONGTEXT does not need read())
            try:
                return str(col)
            except Exception:  # pylint: disable=broad-except
                return col.read()
        return [getval(col) for col in row]

    #Get all jobs with a specified status
    def taskbystatus(self, **kwargs):
        """Retrieves all jobs of the specified user with the specified status"""
        rows = self.api.query(None, None, self.Task.TaskByStatus_sql, username_=kwargs["username"], taskstatus=kwargs["taskstatus"])
        return rows

    #Get all tasks with a specified ddmreqid
    def taskbyddmreqid(self, **kwargs):
        """Retrieves all tasks with the specified ddmreqid"""
        rows = self.api.query(None, None, self.Task.TaskByDdmReqid_sql, ddmreqid=kwargs["ddmreqid"])
        return rows

    def webdir(self, **kwargs):
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")
        workflow = kwargs['workflow']
        try:
            row = self.Task.ID_tuple(*next(self.api.query(None, None, self.Task.ID_sql, taskname=workflow)))
        except StopIteration as e:
            raise ExecutionError("Impossible to find task %s in the database." % kwargs["workflow"]) from e
        yield row.user_webdir

    def getpublishurl(self, **kwargs):
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")
        try:
            row = next(self.api.query(None, None, self.Task.GetPublishUrl_sql, taskname=kwargs['workflow']))
        except StopIteration as e:
            raise ExecutionError("Impossible to find task %s in the database." % kwargs['workflow']) from e
        yield row

    @conn_handler(services=['centralconfig'])
    def webdirprx(self, **kwargs):
        """ Returns the proxied url for the schedd if the schedd has any, returns an empty list instead. Raises in case of other errors.
            To test it use:
            curl -X GET 'https://mmascher-dev6.cern.ch/crabserver/dev/task?subresource=webdirprx&workflow=150224_230633:mmascher_crab_testecmmascher-dev6_3'\
                -k --key /tmp/x509up_u8440 --cert /tmp/x509up_u8440 -v
        """
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")
        workflow = kwargs['workflow']
        self.logger.info("Getting proxied url for %s", workflow)

        try:
            row = self.Task.ID_tuple(*next(self.api.query(None, None, self.Task.ID_sql, taskname=workflow)))
        except StopIteration as e:
            raise ExecutionError("Impossible to find task %s in the database." % kwargs["workflow"]) from e

        if row.user_webdir:
            #extract /cms1425/taskname from the user webdir
            suffix = re.search(r"(/[^/]+/[^/]+/?)$", row.user_webdir).group(0)
        else:
            yield "None"
            return

        #=============================================================================
        # scheddObj is a dictionary composed like this (see the value of htcondorSchedds):
        # "htcondorSchedds": {
        #  "crab3-5@vocms059.cern.ch": {
        #      "proxiedurl": "https://cmsweb.cern.ch/scheddmon/5"
        #  },
        #  ...
        # }
        # so that they have a "proxied URL" to be used in case the schedd is
        # behind a firewall.
        #=============================================================================
        scheddsObj = self.centralcfg.centralconfig['backend-urls'].get('htcondorSchedds', {})
        self.logger.info("ScheddObj for task %s is: %s\nSchedd used for submission %s", workflow, scheddsObj, row.schedd)
        #be careful that htcondorSchedds could be a list (backward compatibility). We might want to remove this in the future
        if row.schedd in list(scheddsObj) and isinstance(scheddsObj, dict):
            self.logger.debug("Found schedd %s", row.schedd)
            proxiedurlbase = scheddsObj[row.schedd].get('proxiedurl')
            self.logger.debug("Proxied url base is %s", proxiedurlbase)
            if proxiedurlbase:
                yield proxiedurlbase + suffix
        else:
            self.logger.info("Could not determine proxied url for task %s", workflow)

    def counttasksbystatus(self, **kwargs):
        """Retrieves all jobs of the specified user with the specified status
           curl -X GET 'https://mmascher-dev6.cern.ch/crabserver/dev/task?subresource=counttasksbystatus&minutes=100'\
                        -k --key /tmp/x509up_u8440 --cert /tmp/x509up_u8440 -v
        """
        if 'minutes' not in kwargs:
            raise InvalidParameter("The parameter minutes is mandatory for the tasksbystatus api")
        rows = self.api.query(None, None, self.Task.CountLastTasksByStatus, minutes=kwargs["minutes"])

        return rows

    def counttasksbyuserandstatus(self, **kwargs):
        """Count jobs for every user and status in the last :minutes
           curl -X GET 'https://cmsweb-test11.cern.ch/crabserver/dev/task?subresource=counttasksbyuserandstatus&minutes=100'\
                        --cert $X509_USER_PROXY --key $X509_USER_PROXY
        """
        if 'minutes' not in kwargs or not kwargs['minutes']:
            raise InvalidParameter("The parameter minutes is mandatory for the tasksbystatus api")
        rows = self.api.query(None, None, self.Task.CountLastTasksByUserAndStatus_sql, minutes=kwargs["minutes"])

        return rows

    def lastfailures(self, **kwargs):
        """Retrieves all jobs of the specified user with the specified status
           curl -X GET 'https://mmascher-dev6.cern.ch/crabserver/dev/task?subresource=lastfailures&minutes=100'\
                        -k --key /tmp/x509up_u8440 --cert /tmp/x509up_u8440 -v
        """
        if 'minutes' not in kwargs:
            raise InvalidParameter("The parameter minutes is mandatory for the tasksbystatus api")
        rows = self.api.query(None, None, self.Task.LastFailures, minutes=kwargs["minutes"])

        for row in rows:
            yield [row[0], row[1], row[2].read()]

    def lastrefused(self, **kwargs):
        """Retrieves all tasks with SUBMITREFUSED status
        curl -X GET 'https://mmascher-dev6.cern.ch/crabserver/dev/task?subresource=lastrefused&minutes=100'\
                        -k --key /tmp/x509up_u8440 --cert /tmp/x509up_u8440 -v
        """
        # Check if 'minutes' parameter is provided
        if 'minutes' not in kwargs:
            raise InvalidParameter("The parameter minutes is mandatory for the lastrefused api")

        # Call LastRefused to fetch tasks with status SUBMITREFUSED
        rows = self.api.query(None, None, self.Task.LastRefused, minutes=kwargs["minutes"])

        # Iterate over the result and fetch tm_task_warnings in row[2]
        for row in rows:
            yield [row[0], row[1], row[2].read()]


    @restcall
    def post(self, subresource, **kwargs):
        """ Updates task information """

        return getattr(RESTTask, subresource)(self, **kwargs)

    def addwarning(self, **kwargs):
        """ Add a warning to the warning column in the database. Can be tested with:
            curl -X POST https://mmascher-poc.cern.ch/crabserver/dev/task -k --key /tmp/x509up_u8440 --cert /tmp/x509up_u8440 \
                    -d 'subresource=addwarning&workflow=140710_233424_crab3test-5:mmascher_crab_HCprivate12&warning=blahblah' -v
        """
        #check if the parameters are there
        if 'warning' not in kwargs or not kwargs['warning']:
            raise InvalidParameter("Warning message not found in the input parameters")
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")

        #decoding and setting the parameters
        workflow = kwargs['workflow']
        authz_owner_match(self.api, [workflow], self.Task) #check that I am modifying my own workflow

#        rows = self.api.query(None, None, "SELECT tm_task_warnings FROM tasks WHERE tm_taskname = :workflow", workflow=workflow)#self.Task.TASKSUMMARY_sql)
        rows = self.api.query(None, None, self.Task.ID_sql, taskname=workflow)#self.Task.TASKSUMMARY_sql)
        rows = list(rows) #from generator to list
        if len(rows)==0:
            raise InvalidParameter("Task %s not found in the task database" % workflow)

        row = self.Task.ID_tuple(*rows[0])
        warnings = literal_eval(row.task_warnings.read() if row.task_warnings else '[]')
        if kwargs['warning'] in warnings:
            self.logger.info("Warning message already present in the task database. Will not add it again.")
            return []
        if len(warnings)>10:
            raise NotAcceptable("You cannot add more than 10 warnings to a task")
        warnings.append(kwargs['warning'])

        self.api.modify(self.Task.SetWarnings_sql, warnings=[str(warnings)], workflow=[workflow])

        return []

    def deletewarnings(self, **kwargs):
        """ Deleet warnings from the warning column in the database. Can be tested with:
            curl -X POST https://mmascher-poc.cern.ch/crabserver/dev/task -k --key /tmp/x509up_u8440 --cert /tmp/x509up_u8440 \
                    -d 'subresource=deletewarnings&workflow=140710_233424_crab3test-5:mmascher_crab_HCprivate12' -v
        """
        #check if the parameter is there
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")

        #decoding and setting the parameters
        workflow = kwargs['workflow']
        authz_owner_match(self.api, [workflow], self.Task) #check that I am modifying my own workflow

#        rows = self.api.query(None, None, "SELECT tm_task_warnings FROM tasks WHERE tm_taskname = :workflow", workflow=workflow)#self.Task.TASKSUMMARY_sql)
        rows = self.api.query(None, None, self.Task.ID_sql, taskname=workflow)#self.Task.TASKSUMMARY_sql)
        rows = list(rows) #from generator to list
        if len(rows)==0:
            raise InvalidParameter("Task %s not found in the task database" % workflow)

        row = self.Task.ID_tuple(*rows[0])
        warnings = literal_eval(row.task_warnings.read() if row.task_warnings else '[]')
        if len(warnings)<1:
            self.logger.info('deletewarnings called for task %s but there are no warnings', workflow)

        self.api.modify(self.Task.DeleteWarnings_sql, workflow=[workflow])

        return []

    def updateschedd(self, **kwargs):
        """ Change scheduler for task submission.
            curl -X POST https://balcas-crab.cern.ch/crabserver/dev/task -ks --key $X509_USER_PROXY --cert $X509_USER_PROXY --cacert $X509_USER_PROXY \
                 -d 'subresource=updateschedd&workflow=150316_221646:jbalcas_crab_test_submit-5-274334&scheddname=vocms095.asdadasdasdacern.ch' -v
        """
        if 'scheddname' not in kwargs or not kwargs['scheddname']:
            raise InvalidParameter("Schedd name not found in the input parameters")
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")

        workflow = kwargs['workflow']
        authz_owner_match(self.api, [workflow], self.Task) #check that I am modifying my own workflow

        self.api.modify(self.Task.UpdateSchedd_sql, scheddname=[kwargs['scheddname']], workflow=[workflow])

        return []

    def updatepublicationtime(self, **kwargs):
        """ Change last publication time for task.
            curl -X POST 'https://mmascher-gwms.cern.ch/crabserver/dev/task' -ks --key $X509_USER_PROXY --cert $X509_USER_PROXY --cacert $X509_USER_PROXY \
                    -d 'subresource=updatepublicationtime&workflow=161128_202743:mmascher_crab_test_preprodaso_preprodorammascher-gwms_0' -v
        """
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")

        workflow = kwargs['workflow']
        authz_owner_match(self.api, [workflow], self.Task) #check that I am modifying my own workflow

        self.api.modify(self.Task.UpdatePublicationTime_sql, workflow=[workflow])

        return []

    def addwebdir(self, **kwargs):
        """ Add web directory to web_dir column in the database. Can be tested with:
            curl -X POST https://balcas-crab.cern.ch/crabserver/dev/task -k --key $X509_USER_PROXY --cert $X509_USER_PROXY \
                    -d 'subresource=addwebdir&workflow=140710_233424_crab3test-5:mmascher_crab_HCprivate12&webdirurl=http://cmsweb.cern.ch/crabserver/testtask' -v
        """
        #check if the parameters are there
        if 'webdirurl' not in kwargs or not kwargs['webdirurl']:
            raise InvalidParameter("Web directory url not found in the input parameters")
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")

        workflow = kwargs['workflow']
        authz_owner_match(self.api, [workflow], self.Task) #check that I am modifying my own workflow

        self.api.modify(self.Task.UpdateWebUrl_sql, webdirurl=[kwargs['webdirurl']], workflow=[workflow])

        return []

    def addoutputdatasets(self, **kwargs):
        if 'outputdatasets' not in kwargs or not kwargs['outputdatasets']:
            raise InvalidParameter("Output datasets not found in the input parameters")
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")

        workflow = kwargs['workflow']
        authz_owner_match(self.api, [workflow], self.Task) #check that I am modifying my own workflow

        row = self.Task.ID_tuple(*next(self.api.query(None, None, self.Task.ID_sql, taskname=workflow)))
        outputdatasets = literal_eval(row.output_dataset.read() if row.output_dataset else '[]')
        outputdatasets = str(list(set(outputdatasets + literal_eval(str(kwargs['outputdatasets'])))))

        self.api.modify(self.Task.SetUpdateOutDataset_sql, tm_output_dataset=[outputdatasets], tm_taskname=[workflow])
        return []

    def addddmreqid(self, **kwargs):
        """ Add DDM request ID to DDM_reqid column in the database. Can be tested with:
            curl -X POST https://balcas-crab.cern.ch/crabserver/dev/task -k --key $X509_USER_PROXY --cert $X509_USER_PROXY \
                    -d 'subresource=addddmreqid&workflow=?&taskstatus=TAPERECALL&ddmreqid=d2b715f526e14f91b0c299abb560d5d7' -v
        """
        #check if the parameters are there
        if 'ddmreqid' not in kwargs or not kwargs['ddmreqid']:
            raise InvalidParameter("DDM request ID not found in the input parameters")
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")

        workflow = kwargs['workflow']
        authz_owner_match(self.api, [workflow], self.Task) #check that I am modifying my own workflow

        self.api.modify(self.Task.UpdateDDMReqId_sql, taskstatus=[kwargs['taskstatus']], ddmreqid=[kwargs['ddmreqid']], workflow=[workflow])
        return []

    def addrucioasoinfo(self, **kwargs):
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")
        if 'transfercontainer' not in kwargs or not kwargs['transfercontainer']:
            raise InvalidParameter("Transfer container name not found in the input parameters")
        if 'transferrule' not in kwargs or not kwargs['transferrule']:
            raise InvalidParameter("Transfer container's rule id not found in the input parameters")
        # For backward compatiblity, either `publishrule` or `multipubrulejson`
        # is enough, also set default value for variables not supplied by
        # client.
        if (('publishrule' not in kwargs or not kwargs['publishrule'])
           and ('multipubrulejson' not in kwargs or not kwargs['multipubrulejson'])):
            raise InvalidParameter("Neither `publishrule` nor `multipubrulejson` are found in the input parameters.")
        # set default value if neither `publishrule` nor `multipubrule` exists
        if 'publishrule' not in kwargs or not kwargs['publishrule']:
            publishrule = '0'*32
        else:
            publishrule = kwargs['publishrule']
        if 'multipubrulejson' not in kwargs or not kwargs['multipubrulejson']:
            multipubrulejson = '{}'
        else:
            multipubrulejson = kwargs['multipubrulejson']
        taskname = kwargs['workflow']
        ownerName = getUsernameFromTaskname(taskname)
        authz_operator(username=ownerName, group='crab3', role='operator')
        self.api.modify(
            self.Task.SetRucioASOInfo_sql,
            tm_transfer_container=[kwargs['transfercontainer']],
            tm_transfer_rule=[kwargs['transferrule']],
            tm_publish_rule=[publishrule],
            tm_multipub_rule=[multipubrulejson],
            tm_taskname=[taskname])
        return []
    def edit(self, **kwargs):
        """
        edit one column in tasks table for the given Task Name
        only operators or task owners can do

        THis can be used by clients as the following
        data={'subresource': 'edit', 'workflow': '250308_212448:belforte_crab_20250308_222239',
           'column': 'tm_num_jobs', 'value': 33}
        result = crabserver.post(api='task', data=urlencode(data))

        CARE IS NEEDED FROM USERS, since checking of column and value is very limited
        """
        #check if the parameters are there (likely useless since it was
        if 'workflow' not in kwargs or not kwargs['workflow']:
            raise InvalidParameter("Task name not found in the input parameters")
        if 'column' not in kwargs or not kwargs['column']:
            raise InvalidParameter("Missing in the input parameter: column")
        if 'value' not in kwargs or not kwargs['value']:
            raise InvalidParameter("Missing input parameter: value")

        #decoding and setting the parameters
        workflow = kwargs['workflow']
        column = kwargs['column']
        value = kwargs['value']
        # next line authrizes task owner and operators
        authz_owner_match(self.api, [workflow], self.Task)
        # create an ad-hoc SQL on the fly, so we can change any column w/o having to write one
        # subresource and SQL for each
        editOneColumn_sql = f"UPDATE TASKS SET {column} = :value WHERE tm_taskname = :workflow"
        # do the change
        self.api.modify(editOneColumn_sql, value=[value], workflow=[workflow])
        return []


