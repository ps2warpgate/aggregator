import os
import re
import prometheus_client as prom
import redis
from constants.utils import is_docker
from dotenv import load_dotenv

if not is_docker():
    load_dotenv()


METRICS_PORT = os.getenv('METRICS_PORT') or 8000
REDIS_URL = os.getenv('REDIS_URL') or None

redis_conn = redis.Redis.from_url(url=REDIS_URL)

def get_current_events():
    # Get in-progress events
    running_events = list(redis_conn.scan(match=r'alerts$')) # worlds with events
    running_events2 = list(redis_conn.hscan(name=r'alerts$'))
    print(len(running_events[1:]))
    print(running_events)


# total_events = prom.Counter('total_census_events', 'Total number of recieved events')
# total_events.inc()

#prom.start_http_server(METRICS_PORT)

get_current_events()
