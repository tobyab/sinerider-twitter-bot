import os
from dotenv import load_dotenv
import redis
from flask import Flask, request, redirect, session, url_for, render_template

load_dotenv()

r = redis.from_url(os.environ["REDIS_URL"])
client_secret = os.environ["CLIENT_SECRET"]
client_id = os.environ["CLIENT_ID"]
redirect_uri = os.environ.get("REDIRECT_URI")

auth_url = "https://twitter.com/i/oauth2/authorize"
token_url = "https://api.twitter.com/2/oauth2/token"

app = Flask(__name__)
app.secret_key = os.urandom(50)

