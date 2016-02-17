import datetime
import json
import queue
import subprocess
import threading

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from settings import *

queue = queue.Queue()

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    """
    def on_status(self, status):
        try:
            self.c += 1
        except:
            self.c = 0
        if self.c % 10 == 0:
             print("%s - %d tweets sent" % (datetime.datetime.utcnow(), self.c))

        try:
            # print("%s" % (json.dumps(status._json, ensure_ascii=False)))
            queue.put(json.dumps(status._json, ensure_ascii=False))
        except Exception as e:
            # Catch any unicode errors while printing to console
            # and just ignore them to avoid breaking application.
            pass
        return True

    def on_error(self, status_code):
        print("An error has occured! Status code = %s" % (status_code))
        return True

def writer(q):
    while True:
        item = q.get()
        producer.send_messages(KAFKA_TOPIC, item.encode('utf-8'))

if __name__ == '__main__':
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    client = KafkaClient(KAFKA_HOST)
    producer = SimpleProducer(client)

    t = threading.Thread(target=writer, args=(queue,))
    t.daemon = True
    t.start()

    l = StdOutListener()

    while True:
        try:
            stream = Stream(auth, l)
            stream.filter(track=TWITTER_TRACK)
        except Exception as e:
            print(e)
