from itertools import count
import json
import os
from castConnector.blob_logger import BlobLogger
import tempfile
import threading
import openpyxl
import requests
import azure.functions as func
from azure.storage.blob import BlobClient
from decimal import *
from datetime import datetime


class RunContext:

    def __init__(self, runId: str, baseURL: str, domainId: str, apiToken:str, bindingKey: str, ldifResultUrl: str, progressCallbackUrl: str, testConnector: bool, logger: BlobLogger):
        self.runId = runId
        self.apiURL = baseURL + '/domains/' + domainId + '/'
        self.headers = {"Authorization": "Bearer "+apiToken}
        self.bindingKey = bindingKey
        self.ldifResultUrl = ldifResultUrl
        self.progressCallbackUrl = progressCallbackUrl
        self.testConnector = testConnector
        self.logger = logger

    def isRunningInTestMode(self):
        return self.testConnector

    def log(self, message: str):
        self.logger.log(datetime.now().strftime('%d.%m.%Y %H:%M:%S')+ ' - ' + message)

    def logRequest(self, request):
        self.log('INFO: ' + request.request.method + ' ' + request.request.url)
        if request.status_code >= 400:
            self.log('ERROR: status ' + str(request.status_code))
            self.log('ERROR: text ' + request.text)
        else:
            self.log('INFO: status ' + str(request.status_code))



    def exception(self, message: str):
        self.logger.exception(message)


class DataLoaderThread(object): #start and run thread
    def __init__(self, runContext):
        self.runContext: RunContext = runContext

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True
        thread.start()
        runContext.log(
            f'INFO: Cast connector is started. runId: {runContext.runId}')

    def run(self):
        self.loadCastData()

    def loadCastData(self):

        data = self.createLdif()
        self.sendCallbackStatus("IN_PROGRESS", "created ldif file")

        # upload ldif into azure storage
        response = requests.put(self.runContext.ldifResultUrl,
                                headers={'x-ms-blob-type': 'BlockBlob'},
                                data=json.dumps(data))
        if (response.status_code != 200 and response.status_code != 201):
            errorMessage = f'Failed to upload ldif into azure blob. status: {response.status_code}, error: {response.text}'
            self.sendCallbackStatus("FAILED", errorMessage)
            return None

        # send last update status for callback endpoint
        self.sendCallbackStatus(
            "FINISHED", "ldif file stored in azure storage.")
        self.runContext.log(
            f'INFO: Successfully finished Cast connector run. runId={self.runContext.runId}')


    def createLdif(self):
        #set default binding key
        data = {
                "connectorType": "cast-showcase",
                "connectorId": "cast-showcase",
                "connectorVersion": "1.0.0",
                "lxVersion": "1.0.0",
                "description": "Imports CAST data into LeanIX",
                "processingDirection": "inbound",
                "processingMode": "partial",
                "customFields": {},
                "content": self.loadApplicationsContent()
            }
        
        #overwrite from provided binding key
        for key in self.runContext.bindingKey:
            data[key] = self.runContext.bindingKey.get(key)
            
        return data

    def loadApplicationsContent(self):
        self.sendCallbackStatus("IN_PROGRESS", "start reading applications")
        applications = requests.get(self.runContext.apiURL + 'applications', headers=self.runContext.headers)
        self.runContext.logRequest(applications)
        if (applications.status_code != 200 and applications.status_code != 201):
            errorMessage = f'Failed to load applications. status: {applications.status_code}, error: {applications.text}'
            self.sendCallbackStatus("FAILED", errorMessage)
            return None
        content = [] #add applications and recommendations from CAST to content to build ldif
        processedComponents = [] #to ensure recommended components are only added once
        
        app_counter = 1
        total_apps = len(applications.json())
        self.sendCallbackStatus("IN_PROGRESS", str(total_apps) + " applications received with status "+ str(applications.status_code))
        for app in applications.json():
            self.sendCallbackStatus("IN_PROGRESS",'load content for application ' + str(app_counter) + ' of '+ str(total_apps))
            appResponse = requests.get(self.runContext.apiURL + 'applications/' + str(app['id']), headers=self.runContext.headers)
            application = appResponse.json()
            self.runContext.logRequest(appResponse)
            if 'metrics' in application:
                metrics = application['metrics'][0]
            else:
                metrics = {}
            recResponse = requests.get(self.runContext.apiURL + 'applications/' +
                            str(app['id']) + '/recommendation', headers=self.runContext.headers)
            rec = recResponse.json()
            self.runContext.logRequest(recResponse)
            recommendations = []
            for r in rec:
                recommendations += r['recommendations']
                for component in recommendations:
                    if component not in processedComponents:
                        content.append({"id": component["name"], "type": "Component", "data": {}})
                        processedComponents.append(component)

            content.append({"id": app['id'], "type": "Application", "data": {
                "name": app['name'],
                "softwareHealth": float(round(Decimal(metrics['softwareHealth'] * 100), 1)) if 'softwareHealth' in metrics else None,
                "businessImpact": float(round(Decimal(metrics['businessImpact'] * 100), 1)) if 'businessImpact' in metrics else None,
                "technicalDebt": metrics['technicalDebt'] if 'technicalDebt' in metrics else None,
                "softwareResiliency": float(round(Decimal(metrics['softwareResiliency'] * 100), 1)) if 'softwareResiliency' in metrics else None,
                "softwareAgility": float(round(Decimal(metrics['softwareAgility'] * 100), 1)) if 'softwareAgility' in metrics else None,
                "softwareElegance": float(round(Decimal(metrics['softwareElegance'] * 100), 1)) if 'softwareElegance' in metrics else None,
                "roadblocks": metrics['roadblocks'] if 'roadblocks' in metrics else None,
                "cloudEffort": metrics['cloudEffort'] if 'cloudEffort' in metrics else None,

                "cloudReady": float(round(Decimal(metrics['cloudReady'] * 100), 1)) if 'cloudReady' in metrics else None,
                "recommendations": [s['name'] for s in recommendations]
            }})
        
            app_counter += 1

        return content
        

      
    
    def sendCallbackStatus(self, status, message):
        data = json.dumps({"status": status, "message": message})
        self.runContext.log('INFO: sendCallbackStatus: ' + data)#todo remove duplicate logs
        response = requests.post(
            url=self.runContext.progressCallbackUrl,
            headers={'Content-Type': 'application/json'},
            data=data)
        response.raise_for_status()
        if (status == 'FAILED'):
            print(f'ERROR (runId={self.runContext.runId}): {message}')
        else:
            print(f'  do callback: {data}')

        return response


def testConnector(runContext: RunContext) -> func.HttpResponse:
    try:
        applications = requests.get(runContext.apiURL + 'applications', headers=runContext.headers)
        data = json.dumps({"message": f'Sucess: Ready to process Cast data: {runContext.apiURL}'})
        return func.HttpResponse(
            data,
            status_code=applications.status_code,
            mimetype='application/json'
        )
    except Exception as err:
        errorMessage = f'Failed to read Cast data from azure blob. {str(err)}'
        print(errorMessage)
        return func.HttpResponse(
            errorMessage,
            status_code=404)


def main(req: func.HttpRequest) -> func.HttpResponse:
    req_body = req.get_json()
    logger = BlobLogger(req_body['connectorLoggingUrl']
                        if 'connectorLoggingUrl' in req_body else None)

    try:
        runContext = RunContext(
            req_body['runId'] if 'runId' in req_body else None,
            req_body['connectorConfiguration']['Base URL'],
            req_body['connectorConfiguration']['Domain ID'],
            req_body['secretsConfiguration']['API Token'],
            req_body['bindingKey'],
            req_body['ldifResultUrl'] if 'ldifResultUrl' in req_body else None,
            req_body["progressCallbackUrl"] if 'progressCallbackUrl' in req_body else None,
            req_body['testConnector'] if 'testConnector' in req_body else False,
            logger)

        # Check case the function is only called to run a test
        if runContext.isRunningInTestMode():
            return testConnector(runContext)

        # Delegate conversion to a background thread
        DataLoaderThread(runContext)

        # Answer immediately
        return func.HttpResponse(
            json.dumps({'runId': runContext.runId, 'status': 'IN_PROGRESS'}),
            status_code=200,
            mimetype='application/json'
        )

    except KeyError as err:
        logger.exception("Failed to trigger connector")
        logger.log(json.dumps(
            {'error': f'Missing field in connector configuration: {str(err)}'}))
        return func.HttpResponse(
            json.dumps(
                {'error': f'Missing field in connector configuration: {str(err)}'}),
            status_code=400,
            mimetype='application/json')
    except Exception as err:
        logger.exception("Failed to trigger connector")
        logger.log(json.dumps({'error': str(err)}))
        return func.HttpResponse(
            json.dumps({'error': str(err)}),
            status_code=400,
            mimetype='application/json')
