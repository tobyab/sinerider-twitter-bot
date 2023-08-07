import os
import statsd

environment = os.environ.get("PYTHON_ENV")
graphite = os.environ.get("GRAPHITE")

if graphite is None:
    raise ValueError("Graphite host not configured!")

metrics = statsd.StatsClient(graphite, 8125, prefix=f'{environment}.sinerider-twitter-bot.')
