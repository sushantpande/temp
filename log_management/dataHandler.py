import logging
import sys
import time
import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as pq
import os
import shutil
import json
import uuid
from math import ceil

from library.exceptionDef import FailedValidation, NotFound
from library.googleStorage import GoogleStorage

logger = logging.getLogger(__name__)

class DataHandler(object):
    def __init__(self, gsBucketName, gsLogQueryDir, localLogQueryDir, partitionSize, queryTypes):
        self.gs = GoogleStorage(gsBucketName)
        self.gsLogQueryRootDir = gsLogQueryDir.lstrip("gs://{}/".format(gsBucketName))
        self.localLogQueryRootDir = "/tmp/results"
        self.partitionSize = partitionSize
        self.queryTypes = queryTypes

    def getLogIndexByTime(self, logs, timestamp):
        print(timestamp)
        leftIndex = 0
        rightIndex = len(logs)-1

        while leftIndex < rightIndex:
            midIndex = int((leftIndex + rightIndex) / 2)
            currentRoundTimestamp = logs.loc[midIndex]["timestamp"]
            if logs.loc[leftIndex]["timestamp"] == timestamp:
                return [leftIndex, leftIndex]
            elif currentRoundTimestamp == timestamp:
                return [midIndex, midIndex]
            elif logs.loc[rightIndex]["timestamp"] == timestamp:
                return [rightIndex, rightIndex]
            elif currentRoundTimestamp > timestamp:
                if rightIndex == midIndex:
                    ret = [leftIndex, rightIndex]
                    return ret
                rightIndex = midIndex
            else:
                if leftIndex == midIndex:
                    ret = [leftIndex, rightIndex]
                    return ret
                leftIndex = midIndex
        return [leftIndex, rightIndex]

    def getPartsInfoToProcess(self, partFilesInfo, startValue, endValue, mode):
        
        partFiles = list()
        startField = "start%s" % mode
        endField = "end%s" % mode

        for part in partFilesInfo:
            if startValue <= part[startField] and part[endField] <= endValue:
                partFiles.append({"partName": part["partName"], startField: part[startField], endField: part[endField]})
            elif endValue >= part[startField] and endValue <= part[endField]:
                if startValue >= part[startField]:
                    partFiles.append({"partName": part["partName"], startField: startValue, endField: endValue})
                else:
                    partFiles.append({"partName": part["partName"], startField: part[startField], endField: endValue})
            elif startValue >= part[startField] and startValue <= part[endField]:
                if endValue >= part[endField]:
                    partFiles.append({"partName": part["partName"], startField: startValue, endField: part[endField]})
                else:
                    partFiles.append({"partName": part["partName"], startField: startValue, endField: endValue})

        return partFiles
    
    def setupPartFile(self, partFile, queryDir, localQueryDir):

        partFileAbs = '%s/%s' % (queryDir, partFile)
        orcFile = '%s/%s' % (localQueryDir, partFile)

        with open(orcFile, 'wb') as f:
            orcContent = self.gs.readFile(partFileAbs)
            f.write(orcContent)
    
    def slice(self, logDf, startIndex, endIndex):

        if startIndex and endIndex:
            retLogDf = logDf[startIndex:endIndex]
        elif not startIndex and not endIndex:
            retLogDf = logDf
        elif not startIndex:
            retLogDf = logDf[:endIndex]
        elif not endIndex:
            retLogDf = logDf[startIndex:]
        
        return retLogDf

    def dateToStr(self, logs):
        
        for index, log in enumerate(logs):
            if log.get('time', ''):
                log['time'] = str(log['time'])
            if log.get('start', ''):
                log['start'] = str(log['start'])
            if log.get('end', ''):
                log['end'] = str(log['end'])

            logs[index] = log


    def generatePartFileList(self, localQueryDir, queryResultDir, partOffsetInfo, downloadedParts, mode, startValue='*', endValue='*'):

        newParts = list()
        if startValue == '*' and endValue == '*':
            partFiles = partOffsetInfo
        else:
            partFiles = self.getPartsInfoToProcess(partOffsetInfo, startValue, endValue, mode)

        for part in partFiles:
            partFile = part["partName"]
            if partFile not in downloadedParts:
                self.setupPartFile(partFile, queryResultDir, localQueryDir)
                newParts.append(partFile)

        return partFiles, newParts

    def getAccurateIndexOnLeft(self, logs, timestamp, index):

        while True:
            index -= 1
            if index >= 0: 
                if logs.loc[index]["timestamp"] != timestamp:
                    break
            else:
                break

        return index+1

    def getAccurateIndexOnRight(self, logs, timestamp, index):

        while True:
            index += 1
            if index < len(logs):
                if logs.loc[index]["timestamp"] != timestamp:
                    break
            else:
                break

        return index-1

    def updateDownloadedMeta(self, downloadedPartOffsetInfo, downloadedPartMetaFile, partOffsetInfo, newParts):

        newPartsInfo = list()

        for part in partOffsetInfo:
            if part["partName"] in newParts:
                newPartsInfo.append(part)

        downloadedPartOffsetInfo += newPartsInfo

        with open(downloadedPartMetaFile, 'w') as f:
            json.dump(downloadedPartOffsetInfo, f)

    def setupLocalCacheDir(self, localQueryDir, queryDir, metaDataFile):
        metaDir = queryDir+"/meta"
        metaDirFiles = self.gs.listDir(metaDir)
        finalMeta = list()

        print(metaDirFiles)
        if metaDirFiles:
            metaPartFile = metaDirFiles[-1]

            os.mkdir(localQueryDir)
            print(metaPartFile)
            plainMeta = self.gs.readFile(metaPartFile).decode("utf-8")
            plainMetaL = plainMeta.split("\n")
            plainMetaL.pop()

            metaDataList = [json.loads(part) for part in plainMetaL]
            sortedMetaData = sorted(metaDataList, key=lambda part: part["partName"])

            startOffset, endOffset = 0, 0

            for part in sortedMetaData:
                endOffset = startOffset+part["count"]-1
                part["partName"] = part["partName"].split("/")[-1]
                part.update({"startOffset": startOffset, "endOffset": endOffset})
                startOffset = endOffset + 1
                part.pop("count")
                finalMeta.append(part)

            with open(metaDataFile, 'w') as f:
                json.dump(finalMeta, f)

        partOffsetInfo = finalMeta
        downloadedPartOffsetInfo = list()
        downloadedParts = list()

        return partOffsetInfo, downloadedPartOffsetInfo, downloadedParts

    def getSliceIndexes(self, logDf, part, downloadedPartOffsetInfo, mode):

        for downloadedPart in downloadedPartOffsetInfo:
            if part["partName"] == downloadedPart['partName']:
                partStartIndex = downloadedPart['startOffset']
                partEndIndex = downloadedPart['endOffset']
                break

        if mode == "Offset":
            requiredStartIndex = part["startOffset"]
            requiredEndIndex = part["endOffset"]
        else:
            requiredStartTimestamp = part['startTimestamp']
            requiredEndTimestamp = part['endTimestamp']
            requiredStartIndex = self.getAccurateIndexOnLeft(logDf, requiredStartTimestamp, self.getLogIndexByTime(logDf, requiredStartTimestamp)[-1])
            requiredEndIndex = self.getAccurateIndexOnRight(logDf, requiredEndTimestamp, self.getLogIndexByTime(logDf, requiredEndTimestamp)[0])
            partEndIndex = partEndIndex - partStartIndex
            partStartIndex = 0

        startIndex = (requiredStartIndex - partStartIndex)
        endIndex = (partEndIndex - requiredEndIndex) * -1

        return startIndex, endIndex

    def collectLogFromPart(self, dirToLook, partFiles, partOffsetInfo, mode, format):

        retLog = None
        for part in partFiles:

            if format == "orc":
                orcData = orc.ORCFile("%s/%s" % (dirToLook, part["partName"]))
                logDf = orcData.read().to_pandas()
            else:
                table = pq.read_table("%s/%s" % (dirToLook, part["partName"]))
                logDf = table.to_pandas()

            startIndex, endIndex = self.getSliceIndexes(logDf, part, partOffsetInfo, mode)

            if retLog is not None:
                retLog = pd.concat([retLog, self.slice(logDf, startIndex, endIndex)])
            else:
                retLog = self.slice(logDf, startIndex, endIndex)

        return retLog

    def getLogData(self, jobId, startValue, endValue, queryType, mode):

        if queryType not in self.queryTypes:
            raise FailedValidation("Invalid query type")

        if startValue > endValue:
            raise FailedValidation("Invalid range given")

        localQueryDir = '%s/%s_%s' % (self.localLogQueryRootDir, jobId, queryType)
        queryDir = '%s/%s_%s' % (self.gsLogQueryRootDir, jobId, queryType)
        queryResultDir = '%s/result' % (queryDir)
        metaDataFile = '%s/metadata.json' % (localQueryDir)
        downloadedPartMetaFile = '%s/downloaded.json' % (localQueryDir)

        try:
            partOffsetInfo = json.load(open(metaDataFile))
            downloadedPartOffsetInfo = json.load(open(downloadedPartMetaFile))
            downloadedParts = [part["partName"] for part in downloadedPartOffsetInfo]
        except IOError:
            partOffsetInfo, downloadedPartOffsetInfo, downloadedParts = self.setupLocalCacheDir(localQueryDir, queryDir, metaDataFile)

        partFiles, newParts = self.generatePartFileList(localQueryDir, queryResultDir, partOffsetInfo, downloadedParts, mode, startValue, endValue) 

        if newParts:
            self.updateDownloadedMeta(downloadedPartOffsetInfo, downloadedPartMetaFile, partOffsetInfo, newParts)

        retLog = self.collectLogFromPart(localQueryDir, partFiles, downloadedPartOffsetInfo, mode, "orc")

        return retLog

    def getResultData(self, jobId, startValue, endValue, queryType, resultId):
        localResultCacheDir = '%s/%s_%s/%s' % (self.localLogQueryRootDir, jobId, queryType, resultId)
        startValue = startValue-1
        endValue = endValue-1

        with open("%s/metadata.json" % localResultCacheDir) as f:
            partOffsetInfo = json.load(f)

        totalLogCount = partOffsetInfo[-1]["endOffset"]+1

        partFiles = self.getPartsInfoToProcess(partOffsetInfo, startValue, endValue, "Offset")

        retLogDf = self.collectLogFromPart(localResultCacheDir, partFiles, partOffsetInfo, "Offset", "parquet")
        retLog = retLogDf.to_dict(orient="records")
        self.dateToStr(retLog)

        return retLog, totalLogCount

    def splitWrite(self, df, dirToWrite):
        metadata = list()
        dfLen = len(df)
        partSize = self.partitionSize
        curPart = self.partitionSize
        partLen = 1 if dfLen <= partSize else int(ceil((dfLen*1.0) / (partSize*1.0)))

        for partNum in range(partLen):
            partFile = "part-%d.parquet" % partNum
            if partNum == 0 and partLen == 1:
                partDf = df
                start = 0
                end = dfLen-1
            elif partNum == 0:
                partDf = df[:curPart]
                start = 0
                end = curPart-1
            elif partNum == partLen-1:
                partDf = df[curPart-partSize:]
                start = curPart-partSize
                end = dfLen-1
            else:
                partDf = df[curPart-partSize:curPart]
                start = curPart-partSize
                end = curPart-1

            curPart += partSize
            metadata.append({"partName": partFile, "startOffset": start, "endOffset": end})
            table = pa.Table.from_pandas(partDf)
            pq.write_table(table, "%s/%s" % (dirToWrite, partFile))

        return metadata

    def getLogByTimestamp(self, jobId, startValue, endValue, queryType, startOffset, endOffset):

        startOffset = startOffset - 1
        endOffset = endOffset - 1
        resultId = ""

        retLogDf = self.getLogData(jobId, startValue, endValue, queryType, "Timestamp")
        dfLen = len(retLogDf) if retLogDf is not None else 0

        if dfLen:
            resultId = "result_%s" % '_'.join(str(uuid.uuid4()).split('-'))
            localResultCacheDir = '%s/%s_%s/%s' % (self.localLogQueryRootDir, jobId, queryType, resultId)

            os.mkdir(localResultCacheDir)

            partOffsetInfo = self.splitWrite(retLogDf, localResultCacheDir)

            with open("%s/metadata.json" % localResultCacheDir, "w") as f:
                json.dump(partOffsetInfo, f)

            partFiles = self.getPartsInfoToProcess(partOffsetInfo, startOffset, endOffset, "Offset")

            retLogDf = self.collectLogFromPart(localResultCacheDir, partFiles, partOffsetInfo, "Offset", "parquet")
            retLog = retLogDf.to_dict(orient="records")
            self.dateToStr(retLog)
        else:
            retLog = []

        return retLog, resultId, dfLen

    def getLogByOffset(self, jobId, startValue, endValue, queryType):

        if isinstance(startValue, int):
            startValue = startValue-1
            endValue = endValue-1

        retLog = self.getLogData(jobId, startValue, endValue, queryType, "Offset")

        retLog = retLog.to_dict(orient="records")
        self.dateToStr(retLog)

        return retLog

    def removeLogData(self, jobId):
        
        for query in self.queryTypes:
            localQueryDir = '%s/%s_%s' % (self.localLogQueryRootDir, jobId, query)
            queryDir = '%s/%s_%s' % (self.gsLogQueryRootDir, jobId, query)

            self.gs.deleteDir(queryDir)

            try:
                shutil.rmtree(localQueryDir)
            except OSError:
                logger.debug("Empty local cache directory. User might not requested any data at all.")

    def removeResultData(self, jobId, queryType, resultId):

        localResultCacheDir = '%s/%s_%s/%s' % (self.localLogQueryRootDir, jobId, queryType, resultId)

        try:
            shutil.rmtree(localResultCacheDir)
        except OSError:
            raise NotFound("resultId: %s not found" % resultId)
        return 

    def getStats(self, jobId, query):
         
        stats = dict()
        
        gsStatsDir = "%s/%s_%s/stats" % (self.gsLogQueryRootDir, jobId, query)
        gsStatsDirInfo = self.gs.listDir(gsStatsDir)
        fileToDownload = ""
        totalRowCount = 0

        if gsStatsDirInfo:
            for file in gsStatsDirInfo:
                if "part" in file:
                    fileToDownload = file

        print ("--------")
        print (gsStatsDirInfo, fileToDownload)

        if fileToDownload:
            statsStr = self.gs.readFile("%s" % (fileToDownload)).decode("utf-8")
            print (statsStr)
            if statsStr:
                statsInfo = json.loads(statsStr)["value"]
            else:
                statsInfo = 0
            totalRowCount = statsInfo

        stats["totalRowCount"] = totalRowCount


        return stats
