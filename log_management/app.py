import json
import os
import logging

import tornado.httpserver
import tornado.options
import tornado.web
import tornado.websocket
from tornado.ioloop import IOLoop
from tornado.web import asynchronous
from tornado.options import define, options
from tornado_cors import CorsMixin

from library.worker import Worker
from library.setupLog import *
from library.exceptionDef import JobIsInProgress, NotFound, FailedValidation, FailedCreation, Conflict

from logManager import LogManager
from getConfig import config

logger = logging.getLogger(__name__)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r'/log/signatures/(.*)', SignatureHandler),
            (r'/log/signatures', SignaturesHandler),
            (r'/log/jobs/(.*)/(.*)', JobActionHandler),
            (r'/log/jobs/(.*)', JobHandler),
            (r'/log/jobs', JobsHandler),
        ]

        tornado.web.Application.__init__(self, handlers)

class BaseHandler(CorsMixin, tornado.web.RequestHandler):

    CORS_ORIGIN = '*'

    def handleExceptions(self, ex):
        if isinstance(ex, JobIsInProgress):
            self.set_status(202)
            self.finish(str(ex))
        elif isinstance(ex, FailedValidation):
            self.set_status(400)
            self.finish(str(ex))
        elif isinstance(ex, NotFound):
            self.set_status(404)
            self.finish(str(ex))
        elif isinstance(ex, Conflict):
            self.set_status(409)
            self.finish(str(ex))
        elif isinstance(ex, FailedCreation):
            self.set_status(500)
            self.finish(str(ex))
        else: 
            self.set_status(500)
            self.finish(str(ex))

    def sendResult(self,val):
        self.set_status(200)
        if isinstance(val, list):
            self.finish(json.dumps(val))
        else:
            self.finish(val)

    def sendStatus(self):
        self.set_status(200)
        self.finish()

    def sendAccepted(self, ret_val):
        self.set_status(202)
        self.finish(ret_val)

    def sendClientError(self, msg):
        logger.error(msg)
        self.set_status(400)
        self.finish(msg)

class SignatureHandler(BaseHandler):

    @asynchronous
    def get(self, signatureId):

        def to_do():
            signatureInfo = logManager.getSignature(signatureId)
            self.sendResult(signatureInfo)

        worker.do_work(to_do, on_error=self.handleExceptions)

    @asynchronous
    def put(self, signatureId):
        try:
            request_body = json.loads(self.request.body)
        except ValueError:
            self.sendClientError("Invalid request body")
            return

        def to_do():
            signatureInfo = logManager.updateSignature(signatureId, request_body)
            self.sendStatus()

        worker.do_work(to_do, on_error=self.handleExceptions)

    @asynchronous
    def delete(self, signatureId):

        def to_do():
            logManager.removeSignature(signatureId)
            self.sendStatus()

        worker.do_work(to_do, on_error=self.handleExceptions)

class SignaturesHandler(BaseHandler):

    @asynchronous
    def get(self):

        try:
            idToSearch = self.get_argument('id')
        except tornado.web.HTTPError:
            idToSearch = '*'

        try:
            keywordToSearch = self.get_argument('keyword')
        except tornado.web.HTTPError:
            keywordToSearch = '*'

        if idToSearch == '*' and keywordToSearch != '*':
            idToSearch = None
        elif keywordToSearch == '*' and idToSearch != '*':
            keywordToSearch = None

        try:
            startOffset = self.get_argument('startOffset')
            endOffset = self.get_argument('endOffset')
        except tornado.web.HTTPError:
            startOffset = '*'
            endOffset = '*'

        def to_do():
            signatureList = logManager.getSignatures(idToSearch, keywordToSearch, startOffset, endOffset)
            self.sendResult(signatureList)

        worker.do_work(to_do, on_error=self.handleExceptions)

    @asynchronous
    def post(self):
        try:
            request_body = json.loads(self.request.body)
        except ValueError:
            self.sendClientError("Invalid request body")
            return

        def to_do():
            logManager.addSignature(request_body)
            self.sendStatus()

        worker.do_work(to_do, on_error=self.handleExceptions)

class JobActionHandler(BaseHandler):

    @asynchronous
    def get(self, jobId, action):

        def to_do():
            if action == 'result':
                queryType = self.get_argument('type')

                try:
                    mode = self.get_argument('mode')
                except tornado.web.HTTPError:
                    mode = None

                if mode:
                    if mode == 'offset':
                        startValue = int(self.get_argument('startOffset'))
                        endValue = int(self.get_argument('endOffset'))
                        print ("Before")
                        logData = logManager.getDataByOffset(jobId, startValue, endValue, queryType)
                        print ("not Before")
                    elif mode == 'time':
                        startValue = int(self.get_argument('startTimestamp'))
                        endValue = int(self.get_argument('endTimestamp'))
                        startOffset = int(self.get_argument('startOffset'))
                        endOffset = int(self.get_argument('endOffset'))
                        logData = logManager.getDataByTimestamp(jobId, startValue, endValue, queryType, startOffset, endOffset)
                    elif mode == 'result':
                        resultId = self.get_argument('resultId')
                        startValue = int(self.get_argument('startOffset'))
                        endValue = int(self.get_argument('endOffset'))
                        logData = logManager.getResultData(jobId, startValue, endValue, queryType, resultId)
                    else:
                        raise FailedValidation("Invalid mode passed. mode: %s" % mode)
                else:
                    logData = logManager.getDataByOffset(jobId, "*", "*", queryType)

                self.sendResult(logData)

        worker.do_work(to_do, on_error=self.handleExceptions)

    @asynchronous
    def delete(self, jobId, action):

        def to_do():
            queryType = self.get_argument('type')
            resultId = self.get_argument('resultId')
            logManager.removeResult(jobId, queryType, resultId)
            self.sendStatus()
            
        worker.do_work(to_do, on_error=self.handleExceptions)

class JobHandler(BaseHandler):

    @asynchronous
    def get(self, jobId):

        def to_do():
            jobInfo = logManager.getJob(jobId)
            self.sendResult(jobInfo)

        worker.do_work(to_do, on_error=self.handleExceptions)

    @asynchronous
    def delete(self, jobId):

        def to_do():
            logManager.removeJob(jobId)
            self.sendStatus()

        worker.do_work(to_do, on_error=self.handleExceptions)

class JobsHandler(BaseHandler):

    @asynchronous
    def get(self):

        try:
            appName = self.get_argument('appName')
        except tornado.web.HTTPError:
            appName = '*'

        try:
            search = self.get_argument('search')
        except tornado.web.HTTPError:
            search = '*'

        def to_do():
            jobList = logManager.getJobs(appName, search)
            self.sendResult(jobList)

        worker.do_work(to_do, on_error=self.handleExceptions)
 
    @asynchronous
    def post(self):
        try:
            request_body = json.loads(self.request.body)
        except ValueError:
            self.sendClientError("Invalid request body")
            return

        appName = self.get_argument('appName')

        def to_do():
            jobIdInfo = logManager.createJob(request_body, appName)
            self.sendAccepted(jobIdInfo)

        worker.do_work(to_do, on_error=self.handleExceptions)

worker = None
logManager = None

def main():

    global worker
    global logManager

    logFile = "%s%s.log" % (config['logging']['logFilePath'], "RestServer")
    setupLog(logFile, logging.getLevelName(config['logging']['logLevel']), config['logging']['maxBytes'], config['logging']['backupCount'])

    logManager = LogManager(config)
    worker = Worker(config['webserverThreadLimit'])

    define("port", default=config["webserverPort"], help="run on the given port", type=int)
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(4401)

    logger.info("Server started and listening on %d"% options.port)

    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
