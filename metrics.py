import prometheus_client as prom


def get_current_events():
    # Get in-progress events
    pass


total_events = prom.Counter('total_census_events', 'Total number of recieved events')
total_events.inc()


