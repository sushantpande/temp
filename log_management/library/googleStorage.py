from google.cloud.storage import Client
from google.cloud.storage.blob import Blob

import logging

logger = logging.getLogger(__name__)

class GoogleStorage:
	def __init__(self, bucketName):
		client = Client()
		self.bucket = client.get_bucket(bucketName)

	def listDir(self, dirPrefix):
		retItems = list()

		try:
			print(dirPrefix)
			dirIt = self.bucket.list_blobs(prefix=dirPrefix)
			for item in dirIt:
				print(item)
				retItems.append(item.name)

		except Exception as ex:
			logger.debug(ex)

		return retItems

	def readFile(self, file):
		fileCont = b''

		try:
			blob = Blob(file, self.bucket)
                        print ("-----------blob")
                        print (blob, file)
			fileCont = blob.download_as_string()
		except Exception as ex:
			logger.debug(ex)

                print ("------------filecont")
                print (fileCont)
		return fileCont

	def deleteDir(self, directory):
		try:
			self.bucket.delete_blob(directory)
		except Exception as ex:
			logger.debug(ex)
