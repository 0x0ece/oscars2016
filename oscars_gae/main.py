import webapp2
import json
import time
import logging
import urllib
import base64

from google.appengine.ext import ndb
from google.appengine.api import memcache
from datetime import datetime, timedelta

import pubsub_utils
from query_cache import TwitterEntityFreq, query_or_cache

from apiclient import errors

OSCAR_START = datetime(year=2016,
    month = 2,
    day = 28,
    hour = 22,
    minute = 0
    )

MC_OSCARS_TOP10 = 'Oscars_Top10'

class MainPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.write('')

class EntitiesTopPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
        self.response.headers['Access-Control-Allow-Origin'] = '*'
        try:
            data = json.loads(memcache.get(MC_OSCARS_TOP10))
        except:
            self.response.write(json.dumps({'entities':[]}))
            return
        entities = data['entities']
        if not entities:
            self.response.write(json.dumps({'entities':[]}))
            return
        self.response.write(json.dumps({
            'entities': zip(*entities)[0],
            "time": datetime.utcfromtimestamp(data['timestamp']).isoformat()+'.000Z'
            }))

class InitHandler(webapp2.RequestHandler):
    """Initializes the Pub/Sub resources."""
    def __init__(self, request=None, response=None):
        """Calls the constructor of the super and does the local setup."""
        super(InitHandler, self).__init__(request, response)
        self.client = pubsub_utils.get_client()
        # self._setup_topic()
        self._setup_subscription()

    # def _setup_topic(self):
    #     """Creates a topic if it does not exist."""
    #     topic_name = pubsub_utils.get_full_topic_name()
    #     try:
    #         self.client.projects().topics().get(
    #             topic=topic_name).execute()
    #     except errors.HttpError as e:
    #         if e.resp.status == 404:
    #             self.client.projects().topics().create(
    #                 name=topic_name, body={}).execute()
    #         else:
    #             logging.exception(e)
    #             raise

    def _setup_subscription(self):
        """Creates a subscription if it does not exist."""
        subscription_name = pubsub_utils.get_full_subscription_name()
        try:
            self.client.projects().subscriptions().get(
                subscription=subscription_name).execute()
        except errors.HttpError as e:
            if e.resp.status == 404:
                body = {
                    'topic': pubsub_utils.get_full_topic_name(),
                    'pushConfig': {
                        'pushEndpoint': pubsub_utils.get_app_endpoint_url()
                    }
                }
                self.client.projects().subscriptions().create(
                    name=subscription_name, body=body).execute()
            else:
                logging.exception(e)
                raise

    def get(self):
        self.response.write("Done")

class EntitiesDataPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
        self.response.headers['Access-Control-Allow-Origin'] = '*'
        req = self.request.get_all("entities[]",[])
        if len(req) > 10:
            self.error(404)
            self.response.write(json.dumps({'error':"Too many entities requested"}))
            return
        ret = {}
        now = datetime.utcnow()
        prev = now - timedelta(hours = 2)
        if prev > OSCAR_START:
            prev = OSCAR_START
        for i in req:
            entities = query_or_cache(i, prev, now)
            data = list(self._create_list_zerofill(entities, prev, now))
            ret[i] = data
        self.response.write(json.dumps({
            'entities': ret,
            }))

    def _to_timestamp(self, date):
        return int(time.mktime(date.timetuple()))

    def _create_list_zerofill(self, data, start,stop):
        next = self._to_timestamp(start)/10*10+10
        stop = self._to_timestamp(stop)/10*10-180
        now = 0
        prev = 0
        prev_t = next
        for i in data:
            now = self._to_timestamp(i.timestamp)
            while now > next:
                w = 1.*(now - next)/(now - prev_t)
                yield {
                    "time": datetime.utcfromtimestamp(next).isoformat()+'.000Z',
                    "count": int(prev * w + i.frequency * (1-w)),
                    }
                next += 10
            yield {
                "time": i.timestamp.isoformat()+'.000Z',
                "count": int(i.frequency),
            }
            prev = i.frequency
            prev_t = now
            next = now+10
        while next < stop:
            yield {
                "time": datetime.utcfromtimestamp(next).isoformat()+'.000Z',
                "count": 0,
                }
            next += 10

class ReceiveMessage(webapp2.RequestHandler):
    """A handler for push subscription endpoint.."""
    def post(self):
        if pubsub_utils.SUBSCRIPTION_UNIQUE_TOKEN != self.request.get('token'):
            self.response.status = 404
            return

        # Store the message in the datastore.
        message = json.loads(urllib.unquote(self.request.body).rstrip('='))
        message_body = base64.b64decode(str(message['message']['data']))
        message = message_body.split(',')
        d = datetime.strptime(message[0][:-5],'%Y-%m-%dT%H:%M:%S')
        timestamp = time.mktime(d.timetuple())
        message = message[1:]
        entities = zip(message[::2],map(int,message[1::2]))
        data_raw = memcache.get(MC_OSCARS_TOP10)
        if data_raw:
            data = json.loads(memcache.get(MC_OSCARS_TOP10))
        else:
            data = None
        if data is None or data['timestamp'] < timestamp:
            memcache.set(MC_OSCARS_TOP10,json.dumps({
                'timestamp': timestamp,
                'entities': entities
                }))

app = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/entities_top.json',EntitiesTopPage),
    ('/entities_data.json',EntitiesDataPage),
    ('/init_ps', InitHandler),
    ('/receive_message', ReceiveMessage),
], debug=False)