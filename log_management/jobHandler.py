from library.generateInputDirRegex import generate 

from library.exceptionDef import FailedValidation, FailedCreation
import requests
import json
import logging

logger = logging.getLogger(__name__)

class JobHandler(object):

    def __init__(self, k8sMasterUrl, driverPodLabel, logQueryResultsDir, compactedOrcDir, queryTypes, jarInformations, jobServer):
        self.k8sMasterUrl = k8sMasterUrl
        self.logQueryOutDir = logQueryResultsDir
        self.inputDir = compactedOrcDir
        self.queryTypes = queryTypes
        self.jarInformation = jarInformations
        self.jobServer = jobServer
        self.sparkConf = None
        self.labelName = driverPodLabel 

        with open("/etc/config/sparkConf.json") as f:
            self.sparkConf = json.load(f)

    def addJob(self, jobData, signatureData, tagAppName, jobId):

        try:
            startTime = jobData["startTime"]
            endTime = jobData["endTime"]
            histogramWindow = jobData["histogramWindow"]
            plugin = signatureData["plugin"]
            logLevel = signatureData["logLevel"]
            docType = signatureData["docType"]
            keywordsToSearch = signatureData["keywords"]

            dirString = generate(startTime, endTime)
            outputDir = "%s/%s" % (self.logQueryOutDir, jobId)

            standardArgs = [str(startTime), str(endTime), tagAppName, plugin, logLevel, docType, outputDir, self.inputDir, dirString]
            if keywordsToSearch:
                keywordsToSearch = '|'.join(keywordsToSearch)

            headers = {'content-type': 'application/json'}
            
            for query in self.queryTypes:
                id = jobId+'_'+query
                args = [arg for arg in standardArgs]
                if query == 'Histogram':
                    args.append(histogramWindow)

                if keywordsToSearch:
                    args.append(keywordsToSearch)

                args[6] = args[6]+'_'+query

                jarPath = self.jarInformation[query]['jar']
                className = self.jarInformation[query]['class']
                driverPodLabel = "spark.kubernetes.driver.label.{}".format(self.labelName)
                self.sparkConf[driverPodLabel] = id

                payload = {
                           "jarFile": jarPath,
                           "class": className,
                           "args": args,
                           "name": id,
                           "k8sMasterUrl": self.k8sMasterUrl,
                           "conf": self.sparkConf
                          }

                response = requests.post(self.jobServer, headers=headers, data=json.dumps(payload))

                if response.status_code != 200:
                    raise FailedCreation("Failed to create job. Error: %s" % response.text)

            logger.info("Posted spark job, Job Id assigned is %s", jobId)
        except KeyError as ex:
            raise FailedValidation("Failed to create job. Parameter missing %s." % str(ex))

    def removeJob(self, jobId):
        url = "{}?labelName={}&labelValue={}".format(self.jobServer, self.labelName, self.labelValue)
        requests.delete(url)
        logger.info("Job deletion intiated for jobId %s", jobId)
