import json
import re
import uuid

from jobHandler import JobHandler
from dataHandler import DataHandler

from library.dbHandler import DbHandler
from library.exceptionDef import NotFound, JobIsInProgress, Conflict, FailedCreation, FailedValidation 

class LogManager(object):

    def __init__(self, config):
        self.job = JobHandler(config['k8sMasterUrl'],
                             config['driverPodLabel'],
                             config['gsLogQueryResultsDir'],
                             config['compactedOrcDir'],
                             config['queryTypes'],
                             config['jarInformations'],
                             config['jobServer'])

        self.data = DataHandler(config['gsBucketName'],
                           config['gsLogQueryResultsDir'],
                           config['localLogQueryResultsDir'],
                           config['partitionSize'],
                           config['queryTypes'])

        self.db = DbHandler(config['db']['userName'], \
                            config['db']['password'], \
                            config['db']['host'], \
                            config['db']['port'], \
                            config['db']['name'])
        self.queryTypes = config['queryTypes']

    def _validateSignature(self, signatureId):
        signatureData = self.db.getSignatureInfo(signatureId)

        if not signatureData:
            raise NotFound("Signature %s Not found" % signatureId)

        return signatureData

    def _generateSignatureDetails(self, signatureData):
        retData = list()

        for signature in signatureData:
            retData.append(dict(signatureId=signature[0], \
                                plugin=signature[1], \
                                logLevel=signature[2], \
                                docType=signature[3], \
                                keywords=json.loads(signature[4]), \
                                createdTime=str(signature[5]), \
                                lastModifiedTime=str(signature[6])))

        return retData 

    def getSignatures(self, id="*", keyword="*", startOffset="*", endOffset="*"):

        signatures = self._generateSignatureDetails(self.db.getSignaturesInfo())
        signaturesById, signaturesByKeyword = list(), list()

        signatures = sorted(signatures, reverse=True, key = lambda signature: signature["createdTime"]) 

        if id != "*" and id is not None:
            signaturesById = [signature for signature in signatures if re.search(id, signature["signatureId"])]
        elif id == "*":
            signaturesById = signatures

        if keyword != "*" and keyword is not None:
            for signature in signatures:
                for key in signature["keywords"]:
                    if re.search(keyword, key):
                        signaturesByKeyword.append(signature)
                        break
        elif keyword == "*":
            signaturesByKeyword = signatures

        allSignatures = signaturesById

        for signatureByKey in signaturesByKeyword:
            duplicate = False

            for signatureById in signaturesById:
                if signatureByKey["signatureId"] == signatureById["signatureId"]:
                    duplicate = True

            if not duplicate:
                allSignatures.append(signatureByKey)

        totalCount = len(allSignatures)
        if startOffset != "*":
            startOffset = int(startOffset) - 1
            endOffset = int(endOffset)

            if endOffset == len(signatures):
                endOffset = 0

            allSignatures = self.data.slice(allSignatures, startOffset, endOffset)

        return {"totalCount": totalCount, "signatures": allSignatures}

    def getSignature(self, signatureId):
        signatureData = self._generateSignatureDetails(self._validateSignature(signatureId))[0]
        return signatureData

    def _validateSignatureId(self, signatureId):
        
        signaturesData = self.getSignatures()["signatures"]

        if signatureId == '' or signatureId == None: 
            raise FailedValidation("Failed to add signature. SignatureId is empty")

        for signature in signaturesData:
            if signature["signatureId"] == signatureId:
                raise Conflict("Signature id %s already exists" % signatureId)

    def addSignature(self, signatureData):
        try:
            self._validateSignatureId(signatureData["signatureId"])
            self.db.addSignatureInfo(signatureData["signatureId"], signatureData["plugin"], signatureData["logLevel"], signatureData["docType"], json.dumps(signatureData["keywords"]))
        except KeyError as ex:
            raise FailedCreation("Failed to add signature. Parameter missing %s." % str(ex))

    def updateSignature(self, signatureToUpdate, signatureData):
        isUpdateRequired = False
        currentSignatureData = self.getSignature(signatureToUpdate)

        try:
            if signatureToUpdate != signatureData["signatureId"]:
                self._validateSignatureId(signatureData["signatureId"])
                isUpdateRequired = True

            if not isUpdateRequired:
                for prop in signatureData:
                    if currentSignatureData[prop] != signatureData[prop]:
                        isUpdateRequired = True

            if isUpdateRequired:
                self.db.updateSignatureInfo(signatureToUpdate, signatureData["signatureId"], signatureData["plugin"], signatureData["logLevel"], signatureData["docType"], json.dumps(signatureData["keywords"]))
        except KeyError as ex:
            raise FailedCreation("Failed to add signature. Parameter missing %s." % str(ex))

    def removeSignature(self, signatureId):
        self._validateSignature(signatureId)
        self.db.removeSignatureInfo(signatureId)

    def _validateJobNameUniqueness(self, jobName):

        jobs = self.getJobs()

        for job in jobs:
            if job['jobName'] == jobName:
                raise Conflict("Job name %s already exists" % jobName)
        
    def createJob(self, jobData, appName):
        try:
            if "jobId" in jobData.keys():
                jobId = jobData.pop("jobId")

                jobDetails = self.getJob(jobId)
                if jobDetails["logQueryProgress"]["percentage"] != 100 or jobDetails["histogramProgress"]["percentage"] != 100:
                    raise JobIsInProgress('Cannot update the job. It might be still running')

                self.removeJob(jobId)

            jobId = "job_%s" % '_'.join(str(uuid.uuid4()).split('-'))

            jobName = jobData.pop('jobName')
            self._validateJobNameUniqueness(jobName)
            signatureData = self.getSignature(jobData["signatureId"])
            self.job.addJob(jobData, signatureData, appName, jobId)
            self.db.addJobInfo(jobId, jobName, json.dumps(jobData), appName)
            return {'jobId': jobId}
        except KeyError as ex:
            raise FailedValidation("Failed to create job. Parameter missing %s." % str(ex)) 

    def getStatus(self, progressPercentage):
        status = ""
        if progressPercentage == 100:
            status = "COMPLETED"
        elif progressPercentage >= 1 and progressPercentage <= 99:
            status = "RUNNING"
        elif progressPercentage == 0:
            status = "NOT STARTED"
        elif progressPercentage == 999:
            status = "FAILED"
        else:
            status = "UNKNOWN"

        return status

    def _generateJobDetails(self, jobData):
        retJobs = list()
        for job in jobData:
            jobInfo = dict(jobId=job[0], \
                           jobName=job[1], \
                           logQueryProgress=json.loads(job[2]), \
                           histogramProgress=json.loads(job[3]), \
                           createdTime=str(job[4]), \
                           modifiedTime=str(job[5]), \
                           jobDetails=json.loads(job[6]), \
                           appName=job[7], \
                           logQueryStatistics=json.loads(job[8]), \
                           histogramStatistics=json.loads(job[9]))

            for query in self.queryTypes:
                if query == "LogQuery" and jobInfo["logQueryProgress"]["percentage"] == 100 and not jobInfo["logQueryStatistics"]:
                    stats = self.data.getStats(jobInfo["jobId"], query)
                    print ("--------------stats")
                    print (stats)
                    self.db.updateJobStatistics(jobInfo["jobId"], json.dumps(stats), query)
                    jobInfo["logQueryStatistics"] = stats
                elif query == "Histogram" and jobInfo["histogramProgress"]["percentage"] == 100 and not jobInfo["histogramStatistics"]:
                    stats = self.data.getStats(jobInfo["jobId"], query)
                    self.db.updateJobStatistics(jobInfo["jobId"], json.dumps(stats), query)
                    jobInfo["histogramStatistics"] = stats

            jobInfo["logQueryProgress"]["status"] = self.getStatus(jobInfo["logQueryProgress"]["percentage"])
            jobInfo["histogramProgress"]["status"] = self.getStatus(jobInfo["histogramProgress"]["percentage"])

            retJobs.append(jobInfo)

        return retJobs

    def getJobs(self, appName='*', search='*'):
        jobDetails = list()
        plainJobDetails = self.db.getJobsInfo()

        jobs = self._generateJobDetails(plainJobDetails)
        jobs = sorted(jobs, reverse=True, key = lambda job: job["createdTime"])

        if appName == "*":
            jobs = jobs
        else:
            jobs = [job for job in jobs if job["appName"] == appName]

        if search != "*":
            jobs = [job for job in jobs if re.search(search, job["jobName"])]

        return jobs

    def getJob(self, jobId):
        plainJobDetail = self.db.getJobInfo(jobId)
        print ("---plain job details")
        print (plainJobDetail)
        if plainJobDetail:
            jobDetail = self._generateJobDetails(plainJobDetail)[0]
            return jobDetail
        else:
            raise NotFound("Job id '%s' not available" % jobId)

    def _getQueryInfo(self, queryType):
        if queryType == 'LogQuery':
            progressType = 'logQueryProgress'
            statsType = 'logQueryStatistics'
        elif queryType == 'Histogram':
            progressType = 'histogramProgress'
            statsType = 'histogramStatistics'
        else:
            raise NotFound("Invalid query type %s" % queryType)

        return progressType, statsType

    def getResultData(self, jobId, startValue, endValue, queryType, resultId):

        data = dict()

        data["data"], totalRowCount = self.data.getResultData(jobId, startValue, endValue, queryType, resultId)
        data["statistics"] = {"totalRowCount": totalRowCount}

        return data

    def _validateResultAvailablity(self, jobDetail, queryType):
        isAvailable = False
        progressType, statsType = self._getQueryInfo(queryType)
        jobProgress = jobDetail[progressType]["percentage"]

        if jobProgress == 100:
            totalRowCount = jobDetail[statsType]["totalRowCount"]
            if totalRowCount >= 1:
                isAvailable = True
        elif jobProgress==999:
            pass
        else:
            raise JobIsInProgress('Query Job might be still running')

        return isAvailable, statsType

    def getDataByTimestamp(self, jobId, startValue, endValue, queryType, startOffset, endOffset):
        data = dict()
        jobDetail = self.getJob(jobId)

        isAvailable, _ = self._validateResultAvailablity(jobDetail, queryType)

        if isAvailable:
            data["data"], resultId, totalRowCount = self.data.getLogByTimestamp(jobId, startValue, endValue, queryType, startOffset, endOffset)
        else:
            data["data"] = []

        if resultId:
            data["resultId"] = resultId

        data["statistics"] = {"totalRowCount": totalRowCount}

        return data

    def getDataByOffset(self, jobId, startValue, endValue, queryType):
        data = dict()
        print ("1")
        jobDetail = self.getJob(jobId)
        print ("2")

        isAvailable, statsType = self._validateResultAvailablity(jobDetail, queryType)
        print ("3")

        if isAvailable:
            data["data"] = self.data.getLogByOffset(jobId, startValue, endValue, queryType)
        else:
            data["data"] = []
        print ("4")

        data["statistics"] = jobDetail[statsType]
        print ("5")

        return data

    def removeResult(self, jobId, queryType, resultId):
        self.data.removeResultData(jobId, queryType, resultId)

    def removeJob(self, jobId):
        jobInfo = self.getJob(jobId)
        self.job.removeJob(jobId)
        self.db.removeJobInfo(jobId)
        self.data.removeLogData(jobId)
