import base64
import hashlib
import os
import re
import json
import requests
import redis
from requests_oauthlib import OAuth2Session
from flask import Flask, request, redirect, session
from pyairtable import Table
import airtable as airtable
from dotenv import load_dotenv

load_dotenv()

airtable_api_key = os.environ["AIRTABLE_API_KEY"]
airtable_base_id = os.environ["AIRTABLE_BASE_ID"]
r = redis.from_url(os.environ["REDIS_URL"])
client_secret = os.environ["CLIENT_SECRET"]
client_id = os.environ["CLIENT_ID"]
redirect_uri = os.environ.get("REDIRECT_URI")

auth_url = "https://twitter.com/i/oauth2/authorize"
token_url = "https://api.twitter.com/2/oauth2/token"
table = Table(airtable_api_key, airtable_base_id, "Leaderboard")

app = Flask(__name__)
app.secret_key = os.urandom(50)

scopes = ["tweet.read", "users.read", "tweet.write", "offline.access"]

code_verifier = base64.urlsafe_b64encode(os.urandom(30)).decode("utf-8")
code_verifier = re.sub("[^a-zA-Z0-9]+", "", code_verifier)
code_challenge = hashlib.sha256(code_verifier.encode("utf-8")).digest()
code_challenge = base64.urlsafe_b64encode(code_challenge).decode("utf-8")
code_challenge = code_challenge.replace("=", "")


def make_token():
    return OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scopes)


def post_tweet(payload, token):
    print("Tweeting!")
    return requests.request(
        "POST",
        "https://api.twitter.com/2/tweets",
        json=payload,
        headers={
            "Authorization": "Bearer {}".format(token["access_token"]),
            "Content-Type": "application/json",
        },
    )


@app.route("/")
def demo():
    global twitter
    twitter = make_token()
    authorization_url, state = twitter.authorization_url(
        auth_url, code_challenge=code_challenge, code_challenge_method="S256"
    )
    session["oauth_state"] = state
    return redirect(authorization_url)


@app.route("/oauth/callback", methods=["GET"])
def callback():
    code = request.args.get("code")
    token = twitter.fetch_token(
        token_url=token_url,
        client_secret=client_secret,
        code_verifier=code_verifier,
        code=code,
    )
    st_token = '"{}"'.format(token)
    j_token = json.loads(st_token)
    r.set("token", j_token)
    tweet = "Hello, world!"
    payload = {"text": "{}".format(tweet)}
    response = post_tweet(payload, token).json()
    return response


@app.route("/api/zapier-replies", methods=["POST"])
def zapier_replies():
    data = request.json
    created_at = data["created_at"]
    id_str = data["id_str"]
    full_text = data["full_text"]
    source = data["source"]
    in_reply_to_status_id_str = data["in_reply_to_status_id_str"]

    # TODO: find out the actual column names - just mock data for now

    tweet = {
        "created_at": created_at,
        "id_str": id_str,
        "full_text": full_text,
        "source": source,
        "in_reply_to_status_id_str": in_reply_to_status_id_str,
    }

    # airtable.insert({ tweet })

if __name__ == "__main__":
    app.run()