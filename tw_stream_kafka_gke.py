# -*- coding: utf-8 -*-
#
# Oppchain data API documentation 
# created by Xiaoxiao SU on Ven 20 nov 2020 15:42:19 CET.
#
# Processing functions for data from Twitter API
#

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaConsumer, KafkaProducer
import json
from json import dumps
from datetime import datetime


def flatten_dict(dd, separator ='_', prefix =''):
    return { prefix + separator + k if prefix else k : v
    for kk, vv in dd.items()
    for k, v in flatten_dict(vv, separator, kk).items()
    } if isinstance(dd, dict) else { prefix : dd }

def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%a %b %d %H:%M:%S %z %Y")
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))
    
#------------------------------------------------------------------------
#
# get twitter stream data
#
#------------------------------------------------------------------------

class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """
    def __init__(self, API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_SECRET_TOKEN):
        self.api_key = API_KEY
        self.api_secret_key = API_SECRET_KEY
        self.access_token = ACCESS_TOKEN
        self.access_secret_token = ACCESS_SECRET_TOKEN
    
    def stream_tweets(self, hash_tag_list, producer, topic_name):
        # This handles Twitter authentication and the connection to the Twitter Streaming API.
        listener = StdOutListener(producer, topic_name)
        auth = OAuthHandler(self.api_key, self.api_secret_key)
        auth.set_access_token(self.access_token, self.access_secret_token)
        stream = Stream(auth, listener)
        # here we filter the Twitter Streams by the given key word(s)
        stream.filter(track=hash_tag_list)

#------------------------------------------------------------------------
#
# send twitter stream data to kafka
#
#------------------------------------------------------------------------
class StdOutListener(StreamListener):
    """
    Class of a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, producer, topic_name):
        self.producer = producer
        self.topic_name = topic_name
    
    def on_data(self, data):
        print(data)
        dict_data=json.loads(data.encode('utf-8'))
        flatten_data = flatten_dict(dict_data)
        # create new dict by choosing keys
        keys = ['created_at', 'id', 'text', 'source', 'user_screen_name', 'user_location', 'user_url', 'user_followers_count', 'user_friends_count', 'user_listed_count', 'user_favourites_count', 'user_statuses_count', 'user_created_at', 'user_time_zone', 'user_lang', 'retweet_count', 'favorite_count', 'favorited', 'retweeted']
        final_dict_data = {x:flatten_data[x] for x in keys}
        final_dict_data['created_at']=str(normalize_timestamp(final_dict_data['created_at']))
        final_dict_data['user_created_at']=str(normalize_timestamp(final_dict_data['user_created_at']))
        print(final_dict_data)
        #print(self.producer)
        self.producer.send(self.topic_name, json.dumps(final_dict_data))
        return True
        
    def on_error(self, status_code):
        print(status_code)
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False


