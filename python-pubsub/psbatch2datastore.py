#!/usr/bin/env python

import httplib2

from apiclient import discovery
from oauth2client import client as oauth2client
from gcloud import datastore
from pslib import *
from multiprocessing import Process

from datetime import datetime
import time

import sys
import argparse
import uuid

def create_datastore_client(http=None):
    credentials = oauth2client.GoogleCredentials.get_application_default()
    if not http:
        http = httplib2.Http()
    credentials.authorize(http)

    return datastore.Client(credentials=credentials)

class DatastoreBe(object):
    def __init__(self, client_ds, key):
        self.client_ds = client_ds
        self.key = key
        self.__queue = []
        self.__lastwrite = time.time()

    def datastore_cb(self, messages):
        self.__queue += messages
        if len(self.__queue) > 400 or (time.time() - self.__lastwrite) > 10:
            self.__write()

    def __write(self):
        rows = []
        deletes = []
        for message in self.__queue:
            try:
                timestamp, entity, count = message.split(',')
                t = datetime.strptime(timestamp[:-5],'%Y-%m-%dT%H:%M:%S')
                q=self.client_ds.query()
                q.add_filter('entity', '=', entity)
                q.add_filter('timestamp', '=', t)
                q.kind = 'TwitterEntityFreq'
                r=q.fetch()
                for i in r:
                    deletes += [i.key]
                ds_entity = datastore.Entity(key=self.key)
                ds_entity['entity'] = entity.decode('utf-8')
                ds_entity['timestamp'] = t
                try:
                    ds_entity['frequency'] = int(count)
                except ValueError:
                    ds_entity['frequency'] = 0
                rows += [ds_entity]

            except Exception as e:
                print(e)
        retries = 10
        while True:
            try:
                self.client_ds.delete_multi(deletes)
                break
            except Exception as e:
                if retries == 0:
                    #bail-out
                    raise
                delay = (11-retries)**2 * 0.25
                print("Deleting: Received an exception, waiting %.2f"%delay)
                time.sleep(delay)
                retries -= 1
        retries = 10
        while True:
            try:
                self.client_ds.put_multi(rows)
                break
            except Exception as e:
                if retries == 0:
                    #bail-out
                    raise
                delay = (11-retries)**2 * 0.25
                print("Inserting: Received an exception, waiting %.2f"%delay)
                time.sleep(delay)
                retries -= 1
        n_msg = len(self.__queue)
        n_rows = len(rows)
        n_dels = len(deletes)
        print "[%s] Received %s messages, deleted %s rows, inserted %s rows" % (
            'OK' if n_msg==n_rows else '!!', n_msg, n_dels, n_rows)
        self.__queue=[]
        self.__lastwrite = time.time()

def main(argv):
    parser = argparse.ArgumentParser(
        description='A command line interface to move tweets from pubsub to bigquery')
    parser.add_argument('project_name', help='Project name in console')
    parser.add_argument('subscription', help='subscription to read from')
    parser.add_argument('-w','--workers', help='change the number of workers',
        default = 10, type=int)

    args = parser.parse_args(argv[1:])
    pool = [Process(target = worker, args = (args,)) for i in xrange(args.workers)]
    print("Starting pool of %d worker"%args.workers)
    for i in pool:
        i.start()

    for i in pool:
        i.join()

def worker(args):
    client_ds = create_datastore_client()
    client_ps = create_pubsub_client()

    key = client_ds.key('TwitterEntityFreq') #No Name after

    ds_store = DatastoreBe(client_ds, key)

    uid = uuid.uuid4().get_hex()
    
    pull_messages_cb(client_ps, args.project_name, args.subscription, 
        ds_store.datastore_cb, max_messages=100)

if __name__ == '__main__':
    main(sys.argv)