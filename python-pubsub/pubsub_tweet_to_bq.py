#!/usr/bin/env python

from pslib import *

from bigquery import get_client as bigquery_get_client

import json
import tweepy
import sys
import argparse
import uuid

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

def create_bq_client(project_name, http=None):
    credentials = GoogleCredentials.get_application_default()
    return bigquery_get_client(project_name, credentials = credentials)

def bq_init_tweets(project_name, table_name, force_delete = False):
    client_bq = create_bq_client(project_name)
    dataset_name, table_name = table_name.split(':')

    if client_bq.check_table(dataset_name, table_name):
        if force_delete:
            client_bq.delete_table(dataset_name, table_name)
        else:
            print("Table already exist, use --force to recreate")
            return

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
    print("Table {}:{} created".format(dataset_name, table_name))

def bq_store_cb(messages, dataset_name, table_name):
    def tweet_author_name(tweet):
        return tweet.author.screen_name if tweet.author else ''

    def tweet_is_retweet(tweet):
        ret = hasattr(tweet, 'retweeted_status') and tweet.retweeted_status
        return bool(ret)

    def tweet_is_reply(tweet):
        ret = hasattr(tweet, 'in_reply_to_status_id') and tweet.in_reply_to_status_id
        return bool(ret)

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

    n_msg = len(messages)
    n_rows = len(rows)
    print "[%s] Received %s messages, inserted %s rows" % (
        'OK' if n_msg==n_rows else '!!', n_msg, n_rows)

def bq_store_tweets(client, project_name, topic, table_name):
    dataset_name, table_name = table_name
    client_bq = create_bq_client(project_name)

    uid = uuid.uuid4().get_hex()
    subscription = '.'.join([topic,uid])
    create_subscription(client, project_name, topic, subscription, ack_deadline = 60)

    try:
        pull_messages_cb(client, project_name, subscription, callback, [dataset_name, table_name])
    finally:
        delete_subscription(client, project_name, subscription)

def main(argv):
    """Invoke a subcommand."""
    # Main parser setup
    parser = argparse.ArgumentParser(
        description='A command line interface to move tweets from pubsub to bigquery')
    parser.add_argument('project_name', help='Project name in console')

    # Sub command parsers
    sub_parsers = parser.add_subparsers(
        title='List of possible commands', metavar='<command>')

    create_topic_str = 'Create a topic with specified name'
    table_parser = argparse.ArgumentParser(add_help=False)
    table_parser.add_argument('table_name', help='Table name')
    parser_init = sub_parsers.add_parser(
        'init', parents=[table_parser],
        description='Init the BigQuery Table', help='Init the BigQuery Table')
    parser_init.set_defaults(func='init')
    parser_init.add_argument(
        '-f', '--force', action='store_true',
        help='Force to recreate the table')

    parser_run = sub_parsers.add_parser(
        'run', parents=[table_parser],
        description='Run the importer', help='Run the importer')
    parser_run.set_defaults(func='run')
    parser_run.add_argument('topic', help='topic to read from')

    args = parser.parse_args(argv[1:])

    if args.func == 'init':
        bq_init_tweets(args.project_name, args.table_name, args.force)
    elif args.func == 'run':
        client = create_pubsub_client()
        bq_store_tweets(client, args.project_name, args.topic, args.table_name)

if __name__ == '__main__':
    main(sys.argv)