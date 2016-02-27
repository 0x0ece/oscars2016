#!/usr/bin/env python

import argparse
import sys

from pslib import *

def main(argv):
    """Invoke a subcommand."""
    # Main parser setup
    parser = argparse.ArgumentParser(
        description='A sample command line interface for Pub/Sub')
    parser.add_argument('project_name', help='Project name in console')

    topic_parser = argparse.ArgumentParser(add_help=False)
    topic_parser.add_argument('topic', help='Topic name')
    subscription_parser = argparse.ArgumentParser(add_help=False)
    subscription_parser.add_argument('subscription', help='Subscription name')

    # Sub command parsers
    sub_parsers = parser.add_subparsers(
        title='List of possible commands', metavar='<command>')

    list_topics_str = 'List topics in project'
    parser_list_topics = sub_parsers.add_parser(
        'list_topics', description=list_topics_str, help=list_topics_str)
    parser_list_topics.set_defaults(func=list_topics)

    create_topic_str = 'Create a topic with specified name'
    parser_create_topic = sub_parsers.add_parser(
        'create_topic', parents=[topic_parser],
        description=create_topic_str, help=create_topic_str)
    parser_create_topic.set_defaults(func=create_topic)

    delete_topic_str = 'Delete a topic with specified name'
    parser_delete_topic = sub_parsers.add_parser(
        'delete_topic', parents=[topic_parser],
        description=delete_topic_str, help=delete_topic_str)
    parser_delete_topic.set_defaults(func=delete_topic)

    list_subscriptions_str = 'List subscriptions in project'
    parser_list_subscriptions = sub_parsers.add_parser(
        'list_subscriptions',
        description=list_subscriptions_str, help=list_subscriptions_str)
    parser_list_subscriptions.set_defaults(func=list_subscriptions)
    parser_list_subscriptions.add_argument(
        '-t', '--topic', help='Show only subscriptions for given topic')

    create_subscription_str = 'Create a subscription to the specified topic'
    parser_create_subscription = sub_parsers.add_parser(
        'create_subscription', parents=[subscription_parser, topic_parser],
        description=create_subscription_str, help=create_subscription_str)
    parser_create_subscription.set_defaults(func=create_subscription)
    parser_create_subscription.add_argument(
        '-p', '--push_endpoint',
        help='Push endpoint to which this method attaches')

    delete_subscription_str = 'Delete the specified subscription'
    parser_delete_subscription = sub_parsers.add_parser(
        'delete_subscription', parents=[subscription_parser],
        description=delete_subscription_str, help=delete_subscription_str)
    parser_delete_subscription.set_defaults(func=delete_subscription)

    publish_message_str = 'Publish a message to specified topic'
    parser_publish_message = sub_parsers.add_parser(
        'publish_message', parents=[topic_parser],
        description=publish_message_str, help=publish_message_str)
    parser_publish_message.set_defaults(func=publish_message)
    parser_publish_message.add_argument('message', help='Message to publish')

    pull_messages_str = ('Pull messages for given subscription. '
                         'Loops continuously unless otherwise specified')
    parser_pull_messages = sub_parsers.add_parser(
        'pull_messages', parents=[subscription_parser],
        description=pull_messages_str, help=pull_messages_str)
    parser_pull_messages.set_defaults(func=pull_messages)
    parser_pull_messages.add_argument(
        '-n', '--no_loop', action='store_true',
        help='Execute only once and do not loop')

    # Google API setup
    client = create_pubsub_client()

    args = parser.parse_args(argv[1:])
    if args.func == list_topics:
        for topic in list_topics(client, args.project_name):
            print topic
    elif args.func == list_subscriptions:
        for subscription in list_subscriptions(client, args.project_name, args.topic):
            print subscription
    elif args.func == create_topic:
        topic = create_topic(client, args.project_name, args.topic)
        if topic:
            print 'Topic {} was created.'.format(topic['name'])
    elif args.func == delete_topic:
        topic = delete_topic(client, args.project_name, args.topic)
        if topic:
            print 'Topic {} was deleted.'.format(topic)
    elif args.func == create_subscription:
        subscription = create_subscription(client, args.project_name, 
            args.topic, args.subscription, args.push_endpoint)
        if subscription:
            print 'Subscription {} was created.'.format(subscription['name'])
    elif args.func == delete_subscription:
        subscription = delete_subscription(client, args.project_name, args.subscription)
        if subscription:
            print 'Subscription {} was deleted.'.format(subscription)
    elif args.func == publish_message:
        resp = publish_message(client, args.project_name, args.topic, args.message)
        print ('Published a message "{}" to a topic {}. The message_id was {}.'
           .format(args.message, args.topic, resp.get('messageIds')[0]))
    elif args.func == pull_messages:
        for message in pull_messages(client, args.project_name, 
            args.subscription, args.no_loop):
            print message

if __name__ == '__main__':
    main(sys.argv)