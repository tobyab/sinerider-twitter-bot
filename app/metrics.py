import os
import statsd
from dotenv import load_dotenv

load_dotenv()

environment = os.environ.get("PYTHON_ENV")
graphite = os.environ.get("GRAPHITE")
proc_type = os.environ.get("PROC_TYPE")

if graphite is None:
    raise ValueError("Graphite host not configured!")

metrics = statsd.StatsClient(graphite, 8125, prefix=f'{environment}.sinerider-twitter-bot.{proc_type}.')
