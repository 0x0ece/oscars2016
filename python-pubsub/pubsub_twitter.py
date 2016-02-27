#!/usr/bin/env python

from pslib import *

import tweepy
import json
import sys
import argparse

class StreamWatcherListener(tweepy.StreamListener):

    def __init__(self, api=None, client=None, project = None topic=None):
        super(StreamWatcherListener, self).__init__(api)
        self.client = client
        self.project = project
        self.topic = topic

        self.queue = []
        self.last_sent = 0

    def _enqueue(self, item, timestamp_ms):
        self.queue.append( (item.encode('utf-8'), timestamp_ms) )

    def _send(self):
        messages = [
                {
                    'data': base64.urlsafe_b64encode(q),
                    'attributes': {'timestamp_ms': timestamp_ms}
                } for q,timestamp_ms in self.queue
            ]
        publish_message_batch(self.client, self.project, self.topic, messages)
        self.last_sent = time.time()
        self.queue = []

    def on_status(self, status):
        # if hasattr(status, 'retweeted_status'):
        #     return

        self._enqueue(json.dumps(status._json), status.timestamp_ms)
        if len(self.queue) >= 100 or (time.time() - self.last_sent > 5.0):
            print "[%.2f] Sending %s tweets" % (time.time(), len(self.queue))
            self._send()

    def on_error(self, status_code):
        print 'An error has occured! Status code = %s' % status_code
        return True  # keep stream alive

    def on_timeout(self):
        print 'Snoozing Zzzzzz'


def twitter_stream(client, project_name, topic, track_list):
    """Connects to Twitter stream API."""
    topic = get_full_topic_name(project_name, topic)

    print 'Connecting to Twitter...'

    with open('twitter.json') as f:
        twitter_cred = json.load(f)
    auth = tweepy.auth.OAuthHandler(twitter_cred['consumer_key'], twitter_cred['consumer_secret'])
    auth.set_access_token(twitter_cred['access_token'], twitter_cred['access_token_secret'])
    watcher = StreamWatcherListener(client=client, project=project_name, topic=topic)
    stream = tweepy.Stream(auth, watcher, timeout=None)

    track_list = [k for k in track_list.split(',')]
    stream.filter(None, track_list)

def main(argv):
    """Invoke a subcommand."""
    # Main parser setup
    parser = argparse.ArgumentParser(
        description='A command line interface to push tweets in pubsub')
    parser.add_argument('project_name', help = 'Project name in console')
    parser.add_argument('topic_name', help = 'Name of the topic on which to push tweets')
    parser.add_argument('track_list', help = 'Comma separated list of hashtag and mentions to track.')    

    # Google API setup
    client = create_pubsub_client()

    args = parser.parse_args(argv[1:])
    twitter_stream(client, args.project_name, args.topic_name, args.track_list)

if __name__ == '__main__':
    main(sys.argv)