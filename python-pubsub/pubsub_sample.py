#!/usr/bin/env python
# Copyright 2014 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Cloud Pub/Sub sample application."""


import argparse
import base64
import json
import os
import re
import socket
import sys
import time

import httplib2
from apiclient import discovery
from oauth2client import client as oauth2client

import tweepy
from bigquery import get_client as bigquery_get_client

ARG_HELP = '''Available arguments are:
  PROJ list_topics
  PROJ create_topic TOPIC
  PROJ delete_topic TOPIC
  PROJ list_subscriptions
  PROJ create_subscription SUBSCRIPTION LINKED_TOPIC
  PROJ delete_subscription SUBSCRIPTION
  PROJ connect_irc TOPIC SERVER CHANNEL
  PROJ twitter_stream TOPIC TRACK_LIST
  PROJ publish_message TOPIC MESSAGE
  PROJ pull_messages SUBSCRIPTION
  PROJ pull_tweets SUBSCRIPTION
  PROJ bq_init_tweets TABLE
  PROJ bq_store_tweets TABLE SUBSCRIPTION
'''

PUBSUB_SCOPES = ["https://www.googleapis.com/auth/pubsub"]

ACTIONS = ['list_topics', 'list_subscriptions', 'create_topic', 'delete_topic',
           'create_subscription', 'delete_subscription', 'connect_irc',
           'twitter_stream', 'pull_tweets', 'bq_init_tweets', 'bq_store_tweets',
           'publish_message', 'pull_messages']

BOTNAME = 'pubsub-irc-bot/1.0'

PORT = 6667

NUM_RETRIES = 3

BATCH_SIZE = 10

ENV_SERVICE_ACCOUNT = "SERVICE_ACCOUNT_EMAIL"

ENV_PRIVKEY_PATH = "PRIVKEY_PATH"


SERVICE_ACCOUNT_EMAIL = '888441798746-lt9opc5b40qe6suhcsq2bt6cjfmi07f0@developer.gserviceaccount.com'
TWITTER_CRED = {
    'consumer_key': 'cHqglmwZofQQjSk2zTFCw',
    'consumer_secret': '9fk79RFCkmL3G261q7YUkL59dsoAlxc8XvtxWaSs',
    'access_token': '227720254-prOsTtusf1wXkXaPhPnwQDQwd5Otjec0mOrku8MK',
    'access_token_secret': 'aywsA94dz34CI5lXHZhxcBtK939HXNN2bUaV9TYkeLY',
}

def help():
    """Shows a help message."""
    sys.stderr.write(ARG_HELP)


def fqrn(resource_type, project, resource):
    """Returns a fully qualified resource name for Cloud Pub/Sub."""
    return "/{}/{}/{}".format(resource_type, project, resource)

def get_full_topic_name(project, topic):
    """Returns a fully qualified topic name."""
    return fqrn('topics', project, topic)

def get_full_subscription_name(project, subscription):
    """Returns a fully qualified subscription name."""
    return fqrn('subscriptions', project, subscription)

def check_args_length(argv, min):
    """Checks the arguments length and exits when it's not long enough."""
    if len(argv) < min:
        help()
        sys.exit(1)


def list_topics(client, args):
    """Shows the list of current topics."""
    next_page_token = None
    while True:
        params = {
            'query':
                'cloud.googleapis.com/project in (/projects/{})'.format(args[0])
        }
        if next_page_token:
            params['pageToken'] = next_page_token
        resp = client.topics().list(**params).execute(num_retries=NUM_RETRIES)
        for topic in resp['topic']:
            print topic['name']
        next_page_token = resp.get('nextPageToken')
        if not next_page_token:
            break


def list_subscriptions(client, args):
    """Shows the list of current subscriptions."""
    next_page_token = None
    while True:
        params = {
            'query':
                'cloud.googleapis.com/project in (/projects/{})'.format(args[0])
        }
        if next_page_token:
            params['pageToken'] = next_page_token
        resp = client.subscriptions().list(**params).execute(
            num_retries=NUM_RETRIES)
        for subscription in resp['subscription']:
            print json.dumps(subscription, indent=1)
        next_page_token = resp.get('nextPageToken')
        if not next_page_token:
            break


def create_topic(client, args):
    """Creates a new topic."""
    check_args_length(args, 3)
    body = {'name': get_full_topic_name(args[0], args[2])}
    topic = client.topics().create(body=body).execute(num_retries=NUM_RETRIES)
    print 'Topic {} was created.'.format(topic['name'])


def delete_topic(client, args):
    """Deletes a topic."""
    check_args_length(args, 3)
    topic = get_full_topic_name(args[0], args[2])
    client.topics().delete(topic=topic).execute(num_retries=NUM_RETRIES)
    print 'Topic {} was deleted.'.format(topic)


def create_subscription(client, args):
    """Creates a new subscription to a given topic."""
    check_args_length(args, 4)
    body = {'name': get_full_subscription_name(args[0], args[2]),
            'topic': get_full_topic_name(args[0], args[3])}
    subscription = client.subscriptions().create(body=body).execute(
        num_retries=NUM_RETRIES)
    print 'Subscription {} was created.'.format(subscription['name'])


def delete_subscription(client, args):
    """Deletes a subscription."""
    check_args_length(args, 3)
    subscription = get_full_subscription_name(args[0], args[2])
    client.subscriptions().delete(subscription=subscription).execute(
        num_retries=NUM_RETRIES)
    print 'Subscription {} was deleted.'.format(subscription)


class StreamWatcherListener(tweepy.StreamListener):

    def __init__(self, api=None, client=None, topic=None):
        super(StreamWatcherListener, self).__init__(api)
        self.client = client
        self.topic = topic

        self.queue = []
        self.last_sent = 0

    def _enqueue(self, item, timestamp_ms):
        self.queue.append( (item.encode('utf-8'), timestamp_ms) )

    def _send(self):
        body = {
            'topic': self.topic,
            'messages': [
                {
                    'data': base64.urlsafe_b64encode(q),
                    'attributes': {'timestamp_ms': timestamp_ms}
                } for q in self.queue
            ]
        }
        self.client.topics().publishBatch(body=body).execute(num_retries=NUM_RETRIES)
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


def twitter_stream(client, args):
    """Connects to Twitter stream API."""
    check_args_length(args, 4)
    track_list = args[3]
    topic = get_full_topic_name(args[0], args[2])

    print 'Connecting to Twitter...'

    auth = tweepy.auth.OAuthHandler(TWITTER_CRED['consumer_key'], TWITTER_CRED['consumer_secret'])
    auth.set_access_token(TWITTER_CRED['access_token'], TWITTER_CRED['access_token_secret'])
    watcher = StreamWatcherListener(client=client, topic=topic)
    stream = tweepy.Stream(auth, watcher, timeout=None)

    track_list = [k for k in track_list.split(',')]
    stream.filter(None, track_list)


def _check_connection(irc):
    """Checks a connection to an IRC channel."""
    readbuffer = ''
    while True:
        readbuffer = readbuffer + irc.recv(1024)
        temp = readbuffer.split('\n')
        readbuffer = temp.pop()
        for line in temp:
            if "004" in line:
                return
            elif "433" in line:
                sys.err.write('Nickname is already in use.')
                sys.exit(1)


def connect_irc(client, args):
    """Connects to an IRC channel and publishes messages."""
    check_args_length(args, 5)
    server = args[3]
    channel = args[4]
    topic = get_full_topic_name(args[0], args[2])
    nick = 'bot-{}'.format(args[0])
    irc = socket.socket()
    print 'Connecting to {}'.format(server)
    irc.connect((server, PORT))

    irc.send("NICK {}\r\n".format(nick))
    irc.send("USER {} 8 * : {}\r\n".format(nick, BOTNAME))
    readbuffer = ''
    _check_connection(irc)
    print 'Connected to {}.'.format(server)

    irc.send("JOIN {}\r\n".format(channel))
    priv_mark = "PRIVMSG {} :".format(channel)
    p = re.compile(
        r'\x0314\[\[\x0307(.*)\x0314\]\]\x03.*\x0302(http://[^\x03]*)\x03')
    while True:
        readbuffer = readbuffer + irc.recv(1024)
        temp = readbuffer.split('\n')
        readbuffer = temp.pop()
        for line in temp:
            line = line.rstrip()
            parts = line.split()
            if parts[0] == "PING":
                irc.send("PONG {}\r\n".format(parts[1]))
            else:
                i = line.find(priv_mark)
                if i == -1:
                    continue
                line = line[i + len(priv_mark):]
                m = p.match(line)
                if m:
                    line = "Title: {}, Diff: {}".format(m.group(1), m.group(2))
                body = {
                    'topic': topic,
                    'messages': [{'data': base64.urlsafe_b64encode(str(line))}]
                }
                client.topics().publishBatch(body=body).execute(
                    num_retries=NUM_RETRIES)


def publish_message(client, args):
    """Publish a message to a given topic."""
    check_args_length(args, 4)
    topic = get_full_topic_name(args[0], args[2])
    message = base64.urlsafe_b64encode(str(args[3]))
    body = {'topic': topic, 'messages': [{'data': message}]}
    resp = client.topics().publishBatch(body=body).execute(
        num_retries=NUM_RETRIES)
    print ('Published a message "{}" to a topic {}. The message_id was {}.'
           .format(args[3], topic, resp.get('messageIds')[0]))


def pull_messages_callback(msg):
    print msg

def pull_messages(client, args, callback=None):
    """Pulls messages from a given subscription."""

    if callback is None:
        callback = pull_messages_callback

    check_args_length(args, 3)
    subscription = get_full_subscription_name(args[0], args[2])
    body = {
        'subscription': subscription,
        'returnImmediately': False,
        'maxEvents': BATCH_SIZE
    }
    while True:
        try:
            resp = client.subscriptions().pullBatch(body=body).execute(
                num_retries=NUM_RETRIES)
        except Exception as e:
            time.sleep(0.5)
            continue
        responses = resp.get('pullResponses')
        if responses is not None:
            ack_ids = []
            for response in responses:
                message = response.get('pubsubEvent').get('message')
                if message:
                    callback( base64.urlsafe_b64decode(str(message.get('data'))) )
                    ack_ids.append(response.get('ackId'))
            ack_body = {'subscription': subscription, 'ackId': ack_ids}
            client.subscriptions().acknowledge(body=ack_body).execute(
                num_retries=NUM_RETRIES)


def pull_tweets_callback(tweet_json):
    tweet_json = json.loads(tweet_json)
    tweet = tweepy.Status.parse(None, tweet_json)
    print "@%s >> %s" % (tweet.author.screen_name, tweet.text)

def pull_tweets(client, args):
    """Pulls tweets from a given subscription."""
    return pull_messages(client, args, pull_tweets_callback)


def bq_init_tweets(client, args):
    check_args_length(args, 3)
    project_id = args[0]

    dataset_name, table_name = args[2].split(':')

    client_bq = bigquery_get_client(project_id, service_account=SERVICE_ACCOUNT_EMAIL, private_key_file='cert.pem', readonly=False)

    # BigQuery docs:
    # Valid types are "string", "integer", "float", "boolean", and "timestamp". 
    # If the type is omitted, it is assumed to be "string".

    schema = [
        {'name': 'id', 'type': 'INTEGER'},
        {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'nullable'},
        {'name': 'author_name', 'type': 'STRING', 'mode': 'nullable'},
        {'name': 'is_reply', 'type': 'BOOLEAN', 'mode': 'nullable'},
        {'name': 'is_retweet', 'type': 'BOOLEAN', 'mode': 'nullable'},
        {'name': 'text', 'type': 'STRING', 'mode': 'nullable'},
        {'name': 'json', 'type': 'STRING', 'mode': 'nullable'},
    ]
    created = client_bq.create_table(dataset_name, table_name, schema)


def bq_store_tweets(client, args):

    def tweet_author_name(tweet):
        return tweet.author.screen_name if tweet.author else ''

    def tweet_is_retweet(tweet):
        ret = hasattr(tweet, 'retweeted_status') and tweet.retweeted_status
        return bool(ret)

    def tweet_is_reply(tweet):
        ret = hasattr(tweet, 'in_reply_to_status_id') and tweet.in_reply_to_status_id
        return bool(ret)

    check_args_length(args, 4)
    project_id = args[0]

    dataset_name, table_name = args[2].split(':')

    client_bq = bigquery_get_client(project_id, service_account=SERVICE_ACCOUNT_EMAIL, private_key_file='cert.pem', readonly=False)

    subscription = get_full_subscription_name(args[0], args[3])
    body = {
        'subscription': subscription,
        'returnImmediately': False,
        'maxEvents': BATCH_SIZE
    }
    while True:
        try:
            resp = client.subscriptions().pullBatch(body=body).execute(
                num_retries=NUM_RETRIES)
        except Exception as e:
            time.sleep(0.5)
            continue
        responses = resp.get('pullResponses')
        
        if responses is not None:
            ack_ids = []
            messages = []
            for response in responses:
                message = response.get('pubsubEvent').get('message')
                if message:
                    messages.append( base64.urlsafe_b64decode(str(message.get('data'))) )
                    ack_ids.append(response.get('ackId'))
            
            rows = []
            for message in messages:
                try:
                    tweet_json = json.loads(message)
                    tweet = tweepy.Status.parse(None, tweet_json)
                    rows.append({
                        'id': tweet.id,
                        'created_at': float(tweet.timestamp_ms)/1000, 
                        'author_name': tweet_author_name(tweet),
                        'is_reply': tweet_is_reply(tweet),
                        'is_retweet': tweet_is_retweet(tweet),
                        'text': tweet.text,
                        'json': message,
                    })
                except Exception as e:
                    print e

            inserted = client_bq.push_rows(dataset_name, table_name, rows, 'id')

            ack_body = {'subscription': subscription, 'ackId': ack_ids}
            client.subscriptions().acknowledge(body=ack_body).execute(
                num_retries=NUM_RETRIES)

            n_msg = len(messages)
            n_rows = len(rows)
            print "[%s] Received %s messages, inserted %s rows" % ('OK' if n_msg==n_rows else '!!', n_msg, n_rows)


def main(argv):
    """Invokes a subcommand."""
    argparser = argparse.ArgumentParser(add_help=False)
    argparser.add_argument('args', nargs='*', help=ARG_HELP)
    privkey = None
    #with open(os.environ.get(ENV_PRIVKEY_PATH), 'r') as f:
    with open('cert.pem', 'r') as f:
        privkey = f.read()
    #    os.environ.get(ENV_SERVICE_ACCOUNT),
    credentials = oauth2client.SignedJwtAssertionCredentials(
        SERVICE_ACCOUNT_EMAIL,
        privkey,
        PUBSUB_SCOPES)
    http = credentials.authorize(http = httplib2.Http())

    client = discovery.build('pubsub', 'v1beta1a', http=http)
    flags = argparser.parse_args(argv[1:])
    args = flags.args
    check_args_length(args, 2)

    if args[1] in ACTIONS:
        globals().get(args[1])(client, args)
    else:
        help()
        sys.exit(1)


if __name__ == '__main__':
    main(sys.argv)

