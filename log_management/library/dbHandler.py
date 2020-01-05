import psycopg2
import logging
from datetime import datetime
from library.exceptionDef import NotFound

logger = logging.getLogger(__name__)

class DbHandler(object):
    def __init__(self, userName, password, host, port, dbName):
        self.connection = psycopg2.connect(dbname=dbName, user=userName, password=password, host=host, port=port)
        self.connection.autocommit = True
        cursor = self.connection.cursor()

        try:
            cursor.execute("CREATE TABLE LOG_MANAGEMENT_SIGNATURES (signatureId VARCHAR, plugin VARCHAR, logLevel VARCHAR, docType VARCHAR, keywords VARCHAR, createdTime TIMESTAMP WITHOUT TIME ZONE NOT NULL, lastModifiedTime TIMESTAMP WITHOUT TIME ZONE NOT NULL)")
        except Exception:
            logger.debug("Table already created")

        try:
            cursor.execute("CREATE TABLE LOG_MANAGEMENT_JOBS (jobId VARCHAR, jobName VARCHAR, logQueryProgress VARCHAR, histogramProgress VARCHAR, createdTime TIMESTAMP WITHOUT TIME ZONE NOT NULL, lastModifiedTime TIMESTAMP WITHOUT TIME ZONE NOT NULL, jobData VARCHAR, tagAppName VARCHAR, logQueryStatistics VARCHAR, histogramStatistics VARCHAR)")
        except Exception:
            logger.debug("Table already created")

        cursor.close()

    def addSignatureInfo(self, signatureId, plugin, logLevel, docType, keywords):
        cursor = self.connection.cursor()

        currentTime = datetime.now()
        cursor.execute("INSERT INTO LOG_MANAGEMENT_SIGNATURES (signatureId, plugin, logLevel, docType, keywords, createdTime, lastModifiedTime) VALUES (%s, %s, %s, %s, %s, %s, %s)", (signatureId, plugin, logLevel, docType, keywords, (currentTime,), (currentTime,),))

        cursor.close()

    def updateSignatureInfo(self, signatureToUpdate, signatureId, plugin, logLevel, docType, keywords):
        cursor = self.connection.cursor()

        currentTime = datetime.now()

        cursor.execute("UPDATE LOG_MANAGEMENT_SIGNATURES SET signatureId=%s, plugin=%s, logLevel=%s, docType=%s, keywords=%s, lastModifiedTime=%s WHERE signatureId=%s", (signatureId, plugin, logLevel, docType, keywords, (currentTime,), signatureToUpdate,))

        cursor.close()


    def removeSignatureInfo(self, signatureId):
        cursor = self.connection.cursor()

        cursor.execute("DELETE FROM LOG_MANAGEMENT_SIGNATURES WHERE signatureId=%s", (signatureId,))

        cursor.close()

    def getSignaturesInfo(self):
        cursor = self.connection.cursor()

        cursor.execute("SELECT * FROM LOG_MANAGEMENT_SIGNATURES")
        data = cursor.fetchall()

        cursor.close()

        return data

    def getSignatureInfo(self, signatureId):
        cursor = self.connection.cursor()

        cursor.execute("SELECT * FROM LOG_MANAGEMENT_SIGNATURES WHERE signatureId=%s", (signatureId,))
        data = cursor.fetchall()

        cursor.close()

        return data

    def addJobInfo(self, jobId, jobName, jobData, tagAppName):
        cursor = self.connection.cursor()

        currentTime = datetime.now()
        cursor.execute("INSERT INTO LOG_MANAGEMENT_JOBS (jobId, jobName, logQueryProgress, histogramProgress, createdTime, lastModifiedTime, jobData, tagAppName, logQueryStatistics, histogramStatistics) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (jobId, jobName, '{"percentage": 0}', '{"percentage": 0}', (currentTime,), (currentTime,), jobData, tagAppName, "[]", "[]",))

        cursor.close()

    def updateJobProgress(self, jobId, progress, queryType):
        cursor = self.connection.cursor()

        currentTime = datetime.now()

        if queryType == "LogQuery":
            cursor.execute("UPDATE LOG_MANAGEMENT_JOBS SET logQueryProgress=%s, lastModifiedTime=%s WHERE jobId=%s", (progress, (currentTime,), jobId,))
        else:
            cursor.execute("UPDATE LOG_MANAGEMENT_JOBS SET histogramProgress=%s, lastModifiedTime=%s WHERE jobId=%s", (progress, (currentTime,), jobId,))

        cursor.close()

    def updateJobStatistics(self, jobId, statistics, queryType):
        cursor = self.connection.cursor()

        currentTime = datetime.now()

        if queryType == "LogQuery":
            cursor.execute("UPDATE LOG_MANAGEMENT_JOBS SET logQueryStatistics=%s, lastModifiedTime=%s WHERE jobId=%s", (statistics, (currentTime,), jobId,))
        else:
            cursor.execute("UPDATE LOG_MANAGEMENT_JOBS SET histogramStatistics=%s, lastModifiedTime=%s WHERE jobId=%s", (statistics, (currentTime,), jobId,))

        cursor.close()

    def getJobsInfo(self):
        cursor = self.connection.cursor()

        cursor.execute("SELECT * FROM LOG_MANAGEMENT_JOBS")
        data = cursor.fetchall()

        cursor.close()

        return data

    def getJobInfo(self, jobId):
        cursor = self.connection.cursor()

        cursor.execute("SELECT * FROM LOG_MANAGEMENT_JOBS WHERE jobId=%s", (jobId,))
        data = cursor.fetchall()

        cursor.close()

        return data

    def removeJobInfo(self, jobId):
        cursor = self.connection.cursor()

        cursor.execute("DELETE FROM LOG_MANAGEMENT_JOBS WHERE jobId=%s", (jobId,))

        cursor.close()

    def removeTables(self):
        cursor = self.connection.cursor()

        cursor.execute("DROP TABLE LOG_MANAGEMENT_JOBS")
        cursor.execute("DROP TABLE LOG_MANAGEMENT_SIGNATURES")

        cursor.close()
