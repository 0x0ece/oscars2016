from google.appengine.ext import ndb
from google.appengine.api import memcache
from datetime import datetime, timedelta

MC_KEY = "mc_cache_%s"
MC_GUARD = "mc_guard_%s"

class TwitterEntityFreq(ndb.Model):
    """A main model for representing an individual Guestbook entry."""
    entity = ndb.StringProperty(indexed=True)
    timestamp = ndb.DateTimeProperty()
    frequency = ndb.IntegerProperty()

class MockTWE(object):
    def __init__(self, entity):
        self.entity = entity.entity
        self.timestamp = entity.timestamp
        self.frequency = entity.frequency

def query_or_cache(entity, start, stop):
    nq = memcache.get(MC_GUARD%entity)
    data = memcache.get(MC_KEY%entity)
    td = timedelta(seconds = 30)
    if data:
        last = data[-1].timestamp
        if stop - last > td and not nq:
            entities = TwitterEntityFreq.query(
                TwitterEntityFreq.entity == entity, 
                TwitterEntityFreq.timestamp > last, 
                TwitterEntityFreq.timestamp <= stop
                ).order(TwitterEntityFreq.timestamp)
            data += [MockTWE(i) for i in entities]
            memcache.set(MC_GUARD%entity,1,30)
            memcache.set(MC_KEY%entity, data)
    elif not nq:
        entities = TwitterEntityFreq.query(
            TwitterEntityFreq.entity == entity, 
            TwitterEntityFreq.timestamp > start, 
            TwitterEntityFreq.timestamp <= stop
            ).order(TwitterEntityFreq.timestamp)
        data = [MockTWE(i) for i in entities]
        memcache.set(MC_GUARD%entity,1,30)
        memcache.set(MC_KEY%entity, data)
    return [i for i in data if i.timestamp > start and i.timestamp <= stop]