# -*- coding: utf-8 -*-
#
# Oppchain data API documentation 
# created by Xiaoxiao SU on Ven 20 nov 2020 15:42:19 CET.
#
# This is the main function
# It chooses one of the following data source to process:
#       - Google Analytics Reporting API
#       - Twitter streaming API
#
# If the data source is Google Analytics Reporting API:
# it gets data and send to Google Cloud Storage.
# 
# If the data source is Twitter streaming API:
# it get data stream and sends to given kafka broker.
#

from datetime import datetime, timedelta
import google_analytics

import os
import twitter_credentials
from kafka import KafkaConsumer, KafkaProducer
import tw_stream_kafka_gke


def main():
    print("--------------- Oppchain data pipeline ---------------")
    DATA_SOURCE = "twitter_api"

    if (DATA_SOURCE == "ga_report_api"):
        ETL_DATE = datetime.strftime(datetime.now() - timedelta(days = 1),"%Y%m%d")
        EXECUTION_TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")
        print("Data source : " + DATA_SOURCE)
        print("\nExtract date : " + ETL_DATE)
        print("Execution timestamp : " + EXECUTION_TIMESTAMP)

        # --------------------------------
        # 1. Google Report API variables
        # --------------------------------

        # service account credentials for OAuth2: identification
        REPORT_API_KEY_FILE_LOCATION = 'ga-reporting-api-app-57ce6cc76915.json'
        REPORT_API_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

        # variables for batch 
        VIEW_ID = '202222139'
        DATE_RANGE = [{'startDate': datetime.strftime(datetime.now() - timedelta(days = 1),'%Y-%m-%d'), 
                    'endDate': datetime.strftime(datetime.now() - timedelta(days = 1),'%Y-%m-%d')}]

        # tables to process
        LIST_TABLES = ["Session", "ClientInfo", "Parcours", "EventButton", "Geo", "Device", "Mobile"]

        # ---------------------------------
        # 2. Google Cloud Storage variables
        # ---------------------------------

        # service account credentials for OAuth2: identification
        GSC_KEY_FILE_LOCATION = 'credentials-python-google-bucket-storage-implyk8s.json'
        # bucket
        BUCKET = 'imply-k8s'
        print("\nData send to Google Cloud Storage")
        print("Bucket name : " + BUCKET)
        # sub bucket path
        BUCKET_TARGET_PATH = 'google_analytics_api/'
        print("------------------------------------------------------")
        
        # ---------------------------------
        # 3. Run
        # ---------------------------------

        print("\n***** Begin to process tables *****\n")
        
        # process table one by one from LIST_TABLES
        for item in LIST_TABLES:
            TABLE = item
            print("Process table : " + TABLE)
            if (TABLE == "Session"):
                DIMENSIONS = [ # max to 9 dimensions
                        {'name': 'ga:dimension1'},  # Client ID
                        {'name': 'ga:dimension6'},  # IP
                        {'name': 'ga:dimension2'}  # Session ID
                    ]
                METRICS = [
                        {'expression': 'ga:pageviews'},
                        {'expression': 'ga:sessionDuration'}, 
                        {'expression': 'ga:hits'}
                    ]
            elif (TABLE == "ClientInfo"):
                DIMENSIONS = [
                        {'name': 'ga:dimension1'},  
                        {'name': 'ga:userType'},
                        {'name': 'ga:daysSinceLastSession'},
                        {'name': 'ga:language'},
                    ]
                METRICS = [
                        {'expression': 'ga:sessions'}
                    ]
            elif (TABLE == "Parcours"):
                DIMENSIONS = [ 
                        {'name': 'ga:dimension1'},  # Client ID
                        {'name': 'ga:dimension2'},  # Session ID
                        {'name': 'ga:dateHourMinute'},
                        {'name': 'ga:source'},
                        {'name': 'ga:medium'},
                        {'name': 'ga:landingPagePath'},
                        {'name': 'ga:secondPagePath'},
                        {'name': 'ga:exitPagePath'},
                        {'name': 'ga:pagePath'},
                    ]
                METRICS = [
                        {'expression': 'ga:hits'}
                    ]
            elif (TABLE == "EventButton"):
                DIMENSIONS = [ 
                        {'name': 'ga:dimension1'},  # Client ID
                        {'name': 'ga:dimension2'},  # Session ID
                        {'name': 'ga:dateHourMinute'},
                        {'name': 'ga:eventCategory'}
                    ]
                METRICS = [
                        {'expression': 'ga:sessionDuration'},
                        {'expression': 'ga:hits'}
                    ]
            elif (TABLE == "Geo"):
                DIMENSIONS = [ 
                        {'name': 'ga:dimension1'},  # Client ID
                        {'name': 'ga:dimension2'},  # Session ID
                        {'name': 'ga:dimension6'},
                        {'name': 'ga:country'},
                        {'name': 'ga:region'},
                        {'name': 'ga:country'},
                        {'name': 'ga:city'},
                        {'name': 'ga:latitude'},
                        {'name': 'ga:longitude'}
                    ]
                METRICS = [
                        {'expression': 'ga:sessions'}
                    ]
            elif (TABLE == "Device"):
                DIMENSIONS = [ 
                        {'name': 'ga:dimension1'},  # Client ID
                        {'name': 'ga:dimension2'},  # Session ID
                        {'name': 'ga:deviceCategory'},
                        {'name': 'ga:operatingSystem'},
                        {'name': 'ga:browser'}
                    ]
                METRICS = [
                        {'expression': 'ga:sessionDuration'},
                        {'expression': 'ga:hits'}
                    ]
            elif (TABLE == "Mobile"):
                DIMENSIONS = [ 
                        {'name': 'ga:dimension1'},  # Client ID
                        {'name': 'ga:dimension2'},  # Session ID
                        {'name': 'ga:deviceCategory'},
                        {'name': 'ga:mobileDeviceBranding'},
                        {'name': 'ga:mobileDeviceInfo'},
                        {'name': 'ga:browser'}
                    ]
                METRICS = [
                        {'expression': 'ga:sessionDuration'},
                        {'expression': 'ga:hits'}
                    ]
            else:
                print("No table chosen. Please enter a table for extraction!")
                exit(0)
                
            # output file
            OUTPUT_FILE = "ga_api_" + ETL_DATE + "_" + TABLE + "_" + EXECUTION_TIMESTAMP + ".json"
            # blob = path + output file
            BLOB_FILE = BUCKET_TARGET_PATH + OUTPUT_FILE
            print("Blob : " + BLOB_FILE)

            # create reporting api object and get data, transform and save as json 
            reporting_api = google_analytics.ReportingAPI(REPORT_API_KEY_FILE_LOCATION, REPORT_API_SCOPES, VIEW_ID, DATE_RANGE, DIMENSIONS, METRICS)
            analytics = reporting_api.initialize_analyticsreporting()
            response = reporting_api.get_report(analytics)
            header, list_data = reporting_api.transform_response_to_list(response)
            reporting_api.write_to_json(header, list_data, OUTPUT_FILE)

            print("Number of rows processed : " + str(len(list_data)))
            
            # create cloud storage object and push json to it
            gcs_agent = google_analytics.GoogleCloudStorage(GSC_KEY_FILE_LOCATION, BUCKET, BLOB_FILE)
            gcs_agent.upload_to_gcs(OUTPUT_FILE)
            print(TABLE + " processed")
            print("------------------------------------------------------")

    elif (DATA_SOURCE == "twitter_api"):
        print("Get data from " + DATA_SOURCE)
        
        # -----------------------
        # 1. Twitter credentials
        # -----------------------

        # API_KEY

        if os.getenv("TW_API_KEY") is None or os.getenv("TW_API_KEY") == '':
            tw_api_key = twitter_credentials.API_KEY  # from config file: credential.py
        else:
            tw_api_key = os.getenv("TW_API_KEY")      # from env var

        # API_SECRET_KEY

        if os.getenv("TW_API_SECRET_KEY") is None or os.getenv("TW_API_SECRET_KEY") == '':
            tw_api_secret_key = twitter_credentials.API_SECRET_KEY  
        else:
            tw_api_secret_key = os.getenv("TW_API_SECRET_KEY") 

        # ACCESS_TOKEN

        if os.getenv("TW_ACCESS_TOKEN") is None or os.getenv("TW_ACCESS_TOKEN") == '':
            tw_access_token = twitter_credentials.ACCESS_TOKEN  
        else:
            tw_access_token = os.getenv("TW_ACCESS_TOKEN")

        # ACCESS_SECRET_TOKEN

        if os.getenv("TW_ACCESS_SECRET_TOKEN") is None or os.getenv("TW_ACCESS_SECRET_TOKEN") == '':
            tw_access_secret_token = twitter_credentials.ACCESS_SECRET_TOKEN  
        else:
            tw_access_secret_token = os.getenv("TW_ACCESS_SECRET_TOKEN")

        # ----------------
        # 2. Kafka
        # ----------------

        # bootstrap server

        if os.getenv("KAFKA_BOOTSTRAP_SERVER") is None or os.getenv("KAFKA_BOOTSTRAP_SERVER") == '':
            kafka_bootstrap_server = twitter_credentials.KAFKA_BOOTSTRAP_SERVER  
        else:
            kafka_bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER")   
        print("Bootstrap server : ", kafka_bootstrap_server, ". \n")  
        #producer = "kafka"
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server, value_serializer=lambda x:x.encode('utf-8'))

        # topic

        if os.getenv("KAFKA_TOPIC") is None or os.getenv("KAFKA_TOPIC") == '':
            kafka_topic = twitter_credentials.KAFKA_TOPIC  
        else:
            kafka_topic = os.getenv("KAFKA_TOPIC")   
        print("Kafka topic : ", kafka_topic, ". \n")  

        # -------------------
        # 3. Keywords
        # -------------------
        
        if os.getenv("TWITTER_HASHTAG_LIST") is None or os.getenv("TWITTER_HASHTAG_LIST") == '':
            tweet_keyword = twitter_credentials.TWITTER_HASHTAG_LIST  
        else:
            tweet_keyword = os.getenv("TWITTER_HASHTAG_LIST")   
        print("Key word given : ", tweet_keyword, ".")  
        hash_tag_list = tweet_keyword.split(",")
        print("List of key words : ")
        print(hash_tag_list)

        # ----------------
        # 4. Run
        # ----------------        
        tweeter_streamer = tw_stream_kafka_gke.TwitterStreamer(tw_api_key, tw_api_secret_key, tw_access_token, tw_access_secret_token)
        tweeter_streamer.stream_tweets(hash_tag_list, producer, kafka_topic)

    elif (DATA_SOURCE == "" or DATA_SOURCE is None):
        print("No data source provided. \nPlease enter a valid data source name. \nExit program!")
        exit(0)
    else:
        print("The provided data source " + DATA_SOURCE + " is not correct or does not exist.\nPlease enter a valid data source name. \nExit program!")
        exit(0)

if __name__ == '__main__':
    main()