# -*- coding: utf-8 -*-
#
# Oppchain data API documentation 
# created by Xiaoxiao SU on Ven 20 nov 2020 15:42:19 CET.
#
# Processing functions for data from Google Analytics Reporting API
#


import json
from datetime import datetime, timedelta

# for authenticate to google Report API
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

# for upload to google bucket
from google.cloud import storage

#------------------------------------------------------------------------
#
# Class for GA Reporting API
#
#------------------------------------------------------------------------

class ReportingAPI():
    def __init__(self, REPORT_API_KEY_FILE_LOCATION, REPORT_API_SCOPES, VIEW_ID, DATE_RANGE, DIMENSIONS, METRICS):
        # oauth2
        self.key_file_location = REPORT_API_KEY_FILE_LOCATION
        self.scopes = REPORT_API_SCOPES
        # reporting api
        self.view_id = VIEW_ID
        self.date_range = DATE_RANGE
        self.dimensions = DIMENSIONS
        self.metrics = METRICS
    
    def initialize_analyticsreporting(self):
        # initilize an analytics object identified with oauth2
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.key_file_location, self.scopes)
        # Build the service object.
        analytics = build('analyticsreporting', 'v4', credentials=credentials)
        return analytics
                
    def get_report(self, analytics):
        # get response from analytics object
        response = analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': self.view_id,
                        'dateRanges': self.date_range,
                        'dimensions': self.dimensions,
                        'metrics': self.metrics
                    }]
            }
        ).execute()
        return response
    
    def transform_response_to_list(self, response):
        # transform response to list
        for report in response.get('reports', []):
            # header
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', []).get('metricHeaderEntries', [])
            metricHeaderValue = [ sub['name'] for sub in metricHeaders ]
            header = dimensionHeaders+metricHeaderValue

            # list data
            list_data = []
            for row in report.get('data', {}).get('rows', []):
                dimensions = row.get('dimensions', [])
                listValues = row.get('metrics', [])
                metrics = [sub['values'] for sub in listValues]
                line = dimensions + metrics[0]
                list_data.append(line)
        return(header, list_data)
        
    def write_to_json(self, header, list_data, output_file):
        res_final = []        
        for i in range(0, len(list_data)):
            res_line = {}
            for j in range(0, len(header)):
                res_line[header[j]] = list_data[i][j]     
            with open(output_file,'a') as f:
                f.write(json.dumps(res_line)+'\n')
        return True       

#------------------------------------------------------------------------
#
# Class for Google Cloud Storage
#
#------------------------------------------------------------------------
class GoogleCloudStorage():
    def __init__(self, GSC_KEY_FILE_LOCATION, BUCKET, BLOB_FILE):
        self.key_file_location = GSC_KEY_FILE_LOCATION
        self.bucket = BUCKET
        self.blob = BLOB_FILE
        
    def upload_to_gcs(self, output_file):
        client = storage.Client.from_service_account_json(json_credentials_path=self.key_file_location)
        bucket = client.bucket(self.bucket)
        blob = bucket.blob(self.blob)
        blob.upload_from_filename(output_file)
        return True
    
    def list_bucket():
        pass