"""Helper Lib for PubSub. Adapted from Google Cloud Pub/Sub sample application."""

import httplib2

from apiclient import discovery
from oauth2client import client as oauth2client

import base64
import json
import time


NUM_RETRIES = 3
BATCH_SIZE = 10

PUBSUB_SCOPES = ['https://www.googleapis.com/auth/pubsub']

def create_pubsub_client(http=None):
    credentials = oauth2client.GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(PUBSUB_SCOPES)
    if not http:
        http = httplib2.Http()
    credentials.authorize(http)

    return discovery.build('pubsub', 'v1', http=http)

def fqrn(resource_type, project, resource):
    """Return a fully qualified resource name for Cloud Pub/Sub."""
    return "projects/{}/{}/{}".format(project, resource_type, resource)


def get_full_topic_name(project, topic):
    """Return a fully qualified topic name."""
    return fqrn('topics', project, topic)


def get_full_subscription_name(project, subscription):
    """Return a fully qualified subscription name."""
    return fqrn('subscriptions', project, subscription)


def list_topics(client, project_name):
    """Show the list of current topics."""
    next_page_token = None
    while True:
        resp = client.projects().topics().list(
            project='projects/{}'.format(project_name),
            pageToken=next_page_token).execute(num_retries=NUM_RETRIES)
        if 'topics' in resp:
            for topic in resp['topics']:
                yield topic['name']
        next_page_token = resp.get('nextPageToken')
        if not next_page_token:
            break


def list_subscriptions(client, project_name, topic = None):
    """Show the list of current subscriptions.
    If a topic is specified, only subscriptions associated with the topic will
    be listed.
    """
    next_page_token = None
    while True:
        if topic is None:
            resp = client.projects().subscriptions().list(
                project='projects/{}'.format(project_name),
                pageToken=next_page_token).execute(num_retries=NUM_RETRIES)
        else:
            topic = get_full_topic_name(project_name, topic)
            resp = client.projects().topics().subscriptions().list(
                topic=topic,
                pageToken=next_page_token).execute(num_retries=NUM_RETRIES)
        if 'subscriptions' in resp:
            for subscription in resp['subscriptions']:
                yield subscription
        next_page_token = resp.get('nextPageToken')
        if not next_page_token:
            break


def create_topic(client, project_name, topic):
    """Create a new topic."""
    topic = client.projects().topics().create(
        name=get_full_topic_name(project_name, topic),
        body={}).execute(num_retries=NUM_RETRIES)
    return topic
    
def delete_topic(client, project_name, topic):
    """Delete a topic."""
    topic = get_full_topic_name(project_name, topic)
    client.projects().topics().delete(
        topic=topic).execute(num_retries=NUM_RETRIES)
    return topic
   
def create_subscription(client, project_name, topic, subscription, 
        push_endpoint=None, ack_deadline = 10):
    """Create a new subscription to a given topic.
    If an endpoint is specified, this function will attach to that
    endpoint.
    """
    name = get_full_subscription_name(project_name, subscription)
    if '/' in topic:
        topic_name = topic
    else:
        topic_name = get_full_topic_name(project_name, topic)
    body = {
        'topic': topic_name,
        'ackDeadlineSeconds': ack_deadline
        }
    if push_endpoint is not None:
        body['pushConfig'] = {'pushEndpoint': push_endpoint}
    subscription = client.projects().subscriptions().create(
        name=name, body=body).execute(num_retries=NUM_RETRIES)
    return subscription
    
def delete_subscription(client, project_name, subscription):
    """Delete a subscription."""
    subscription = get_full_subscription_name(project_name,
                                              subscription)
    client.projects().subscriptions().delete(
        subscription=subscription).execute(num_retries=NUM_RETRIES)
    return subscription
   
def publish_message(client, project_name, topic, message):
    """Publish a message to a given topic."""
    topic = get_full_topic_name(project_name, topic)
    message = base64.b64encode(str(message))
    body = {'messages': [{'data': message}]}
    resp = client.projects().topics().publish(
        topic=topic, body=body).execute(num_retries=NUM_RETRIES)
    return resp

def publish_message_batch(client, project_name, topic, messages):
    """Publish a list of messages to a given topic."""
    topic = get_full_topic_name(project_name, topic)
    body = {'messages': messages}
    resp = client.projects().topics().publish(
        topic=topic, body=body).execute(num_retries=NUM_RETRIES)
    return resp

def pull_messages(client, project_name, subscription, no_loop = False):
    """Pull messages from a given subscription."""
    subscription = get_full_subscription_name(
        project_name,
        subscription)
    body = {
        'returnImmediately': False,
        'maxMessages': BATCH_SIZE
    }
    while True:
        try:
            resp = client.projects().subscriptions().pull(
                subscription=subscription, body=body).execute(
                    num_retries=NUM_RETRIES)
        except Exception as e:
            time.sleep(0.5)
            yield e
            continue
        receivedMessages = resp.get('receivedMessages')
        if receivedMessages:
            ack_ids = []
            for receivedMessage in receivedMessages:
                message = receivedMessage.get('message')
                if message:
                    yield base64.b64decode(str(message.get('data')))
                    ack_ids.append(receivedMessage.get('ackId'))
            ack_body = {'ackIds': ack_ids}
            client.projects().subscriptions().acknowledge(
                subscription=subscription, body=ack_body).execute(
                    num_retries=NUM_RETRIES)
        if no_loop:
            break

def pull_messages_cb(client, project_name, subscription, callback, cb_args = [], no_loop = False):
    """Pull messages from a given subscription."""
    subscription = get_full_subscription_name(
        project_name,
        subscription)
    body = {
        'returnImmediately': False,
        'maxMessages': BATCH_SIZE
    }
    while True:
        try:
            resp = client.projects().subscriptions().pull(
                subscription=subscription, body=body).execute(
                    num_retries=NUM_RETRIES)
        except Exception as e:
            time.sleep(0.5)
            yield e
            continue
        receivedMessages = resp.get('receivedMessages')
        if receivedMessages:
            ack_ids = []
            messages = []
            for receivedMessage in receivedMessages:
                message = receivedMessage.get('message')
                if message:
                    messages.append(message)
                    ack_ids.append(receivedMessage.get('ackId'))
            callback(messages, *cb_args)
            ack_body = {'ackIds': ack_ids}
            client.projects().subscriptions().acknowledge(
                subscription=subscription, body=ack_body).execute(
                    num_retries=NUM_RETRIES)
        if no_loop:
            break