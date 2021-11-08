import traceback
from azure.storage.blob import BlobClient

# Unfortunately the 'logging' functionality is not usable over the whole lifecycle of the excel
# function because the main thread lives only for a very short of time and the conversion is
# done asynchronously.
# Therefore this implementation just simply prints the content to stdout.
#
# Heads-up: In Visual Code Studio, the output will be found inside the Debugging-Console!


class BlobLogger(object):
    def __init__(self, connectorLoggingUrl):
        try:
            self.blobClient = BlobClient.from_blob_url(
                connectorLoggingUrl) if connectorLoggingUrl else None
            if self.blobClient:
                self.blobClient.create_append_blob()

        except Exception as err:
            self.logger.log(f'Failed to initialize BlobClient. {err.message}')

    def log(self, message):
        print(message)
        if(self.blobClient):
            self.blobClient.append_block(message + '\n')

    def exception(self, message):
        exceptionMsg = traceback.format_exc()
        fullmessage = f'{message}\n{exceptionMsg}'
        print(fullmessage)
        if(self.blobClient):
            self.blobClient.append_block(fullmessage + '\n')