import webapp2
import json
import time

from google.appengine.ext import ndb
from datetime import datetime, timedelta

OSCAR_START = datetime(year=2016,
    month = 2,
    day = 28,
    hour = 22,
    minute = 0
    )

class MainPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.write('')

class EntitiesTopPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
        self.response.write(json.dumps({'entities':[
            "#oscars2016",
            "#oscar2016",
            "@oscar",
            ]}))

class TwitterEntityFreq(ndb.Model):
    """A main model for representing an individual Guestbook entry."""
    entity = ndb.StringProperty(indexed=True)
    timestamp = ndb.DateTimeProperty()
    frequency = ndb.IntegerProperty()

class EntitiesDataPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
        req = self.request.get_all("entities[]",[])
        if len(req) > 10:
            self.error(404)
            self.response.write(json.dumps({'error':"Too many entities requested"}))
            return
        ret = []
        now = datetime.utcnow()
        prev = now - timedelta(hours = 2)
        if prev > OSCAR_START:
            prev = OSCAR_START
        for i in req:
            entities = TwitterEntityFreq.query(
                TwitterEntityFreq.entity == i, 
                TwitterEntityFreq.timestamp > prev, 
                TwitterEntityFreq.timestamp <= now
                ).order(TwitterEntityFreq.timestamp)
            data = list(self._create_list_zerofill(entities, prev, now))
            ret += [{i:data}]
        self.response.write(json.dumps({
            'entities': ret,
            }))

    def _to_timestamp(self, date):
        return int(time.mktime(date.timetuple()))

    def _create_list_zerofill(self, data, start,stop):
        next = self._to_timestamp(start)/10*10+10
        stop = self._to_timestamp(stop)/10*10-60
        now = 0
        for i in data:
            now = self._to_timestamp(i.timestamp)
            while now > next:
                yield {
                    "time": datetime.utcfromtimestamp(next).isoformat()+'.000Z',
                    "count": 0,
                    }
                next += 10
            yield {
                "time": i.timestamp.isoformat()+'.000Z',
                "count": int(i.frequency),
            }
            next = now+10
        while next < stop:
            yield {
                "time": datetime.utcfromtimestamp(next).isoformat()+'.000Z',
                "count": 0,
                }
            next += 10


app = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/entities_top.json',EntitiesTopPage),
    ('/entities_data.json',EntitiesDataPage),
], debug=True)