import json
from kafka import KafkaProducer, KafkaClient,SimpleProducer,SimpleClient
#from pykafk
import tweepy
#import configparser
import pandas as pd

# Note: Some of the imports are external python libraries. They are installed on the current machine.
# If you are running multinode cluster, you have to make sure that these libraries
# and currect version of Python is installed on all the worker nodes.

class TweeterStreamListener(tweepy.StreamListener):
    """ A class to read the twiiter stream and push it to Kafka"""

    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        #client = KafkaClient("localhost:9092")
        print "Initializing kafka producer"
        self.producer = KafkaProducer(bootstrap_servers=['172.31.78.243:9092'])

    def on_status(self, status):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        #msg =  status.text.encode('utf-8')
        msg = json.dumps(status._json)
        #print str(msg)
        try:
            self.producer.send('twitterstream', msg)
        except Exception as e:
            print(e)
            return False
        return True

    def on_error(self, status_code):
        print("Error received in kafka producer")
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream

if __name__ == '__main__':

    print "Initializing Tweeter stream"
    consumer_key = 'vRThG2kPUxonACAgeI1Hfi2je'
    consumer_secret = '5rcsLmZWeLSDzJUiqQTrEBywCNVafnkSugTB1Qcj4ofqPMHRq9'
    access_token = '1431058350-s7chaBxi96ohsO41Shcdj8xY6zWSaP0fuY1arfI'
    access_token_secret = 'W74lT1eUNI8BUUBrKUXC2fk4E00I6bX6NTXezxAeYrunU'

    # Create Auth object
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    nasdaq100 = pd.read_csv('nasdaq100.csv')
    searchTerms = nasdaq100['Search']
    searchList = []
    for search in searchTerms:
        searchList.append(str(search))
    # Create stream and bind the listener to it
    stream = tweepy.Stream(auth, listener = TweeterStreamListener(api))

    print "Starting stream"
    stream.filter(track=searchList,languages = ['en'])