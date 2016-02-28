#!/usr/bin/env python

import httplib2

from apiclient import discovery
from oauth2client import client as oauth2client
from gcloud import datastore
from pslib import *

from datetime import datetime

import sys
import argparse
import uuid

def create_datastore_client(http=None):
    credentials = oauth2client.GoogleCredentials.get_application_default()
    if not http:
        http = httplib2.Http()
    credentials.authorize(http)

    return datastore.Client(credentials=credentials)

def datastore_cb(messages, client_ds, key):
    rows = []
    for message in messages:
        try:
            timestamp, entity, count = message.split(',')
            t = datetime.strptime(timestamp[:-5],'%Y-%m-%dT%H:%M:%S')
            ds_entity = datastore.Entity(key=key)
            ds_entity['entity'] = entity.decode('utf-8')
            ds_entity['timestamp'] = t
            try:
                ds_entity['frequency'] = int(count)
            except ValueError:
                ds_entity['frequency'] = 0
            rows += [ds_entity]

        except Exception as e:
            print(e)
    client_ds.put_multi(rows)
    n_msg = len(messages)
    n_rows = len(rows)
    print "[%s] Received %s messages, inserted %s rows" % (
        'OK' if n_msg==n_rows else '!!', n_msg, n_rows)

def main(argv):
    parser = argparse.ArgumentParser(
        description='A command line interface to move tweets from pubsub to bigquery')
    parser.add_argument('project_name', help='Project name in console')
    parser.add_argument('topic', help='topic to read from')

    args = parser.parse_args(argv[1:])

    client_ds = create_datastore_client()
    client_ps = create_pubsub_client()

    key = client_ds.key('TwitterEntityFreq')

    uid = uuid.uuid4().get_hex()
    subscription = '.'.join([args.topic,uid])
    create_subscription(client_ps, args.project_name, args.topic, subscription, ack_deadline = 60)

    try:
        pull_messages_cb(client_ps, args.project_name, subscription, 
            datastore_cb, [client_ds, key], max_messages=100)
    finally:
        delete_subscription(client_ps, args.project_name, subscription)

if __name__ == '__main__':
    main(sys.argv)