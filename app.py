import base64
import hashlib
import os
import json
import random

import redis
import re
import requests
import polling
from requests_oauthlib import OAuth2Session
from flask import Flask, request, redirect, session
from pyairtable import Table
from dotenv import load_dotenv
from auth import login_required
from pyairtable.formulas import match, EQUAL, AND, IF, FIELD, to_airtable_value
import urllib.parse

print("Starting up...")
load_dotenv()

airtable_api_key = os.environ["AIRTABLE_API_KEY"]
airtable_base_id = os.environ["AIRTABLE_BASE_ID"]
r = redis.from_url(os.environ["REDIS_URL"])
client_secret = os.environ["CLIENT_SECRET"]
client_id = os.environ["CLIENT_ID"]
redirect_uri = os.environ.get("REDIRECT_URI")
scoring_service_uri = os.environ.get("SINERIDER_SCORING_SERVICE")

auth_url = "https://twitter.com/i/oauth2/authorize"
token_url = "https://api.twitter.com/2/oauth2/token"

workQueueTable = Table(airtable_api_key, airtable_base_id, "TwitterWorkQueue")
leaderboardTable = Table(airtable_api_key, airtable_base_id, "Leaderboard")

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


@app.route("/status")
@login_required
def status():
    return "Hello, I am online!  Your IP: " + request.remote_addr


@app.route("/test", methods=["POST"])
@login_required
def test():
    for key, value in request.form.items():
        twitter_req = request.form.get("full_text", "user__name", "id")
        print("request: " + twitter_req)
    return "Thanks!"


@app.route("/")
@login_required
def demo():
    global twitter
    twitter = make_token()
    authorization_url, state = twitter.authorization_url(
        auth_url, code_challenge=code_challenge, code_challenge_method="S256"
    )
    session["oauth_state"] = state
    return redirect(authorization_url)


def parse_puzzle():
    url = "https://sinerider-api.herokuapp.com/daily"
    puzzle = requests.request("GET", url).json()
    return puzzle["facts"][0]


@app.route("/oauth/callback", methods=["GET"])
@login_required
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
    tweet = "Woahhhhh! Here's our latest challenge! Check it out here: " + parse_puzzle()
    payload = {"text": "{}".format(tweet)}
    response = post_tweet(payload, token).json()
    return response


@app.route("/onNewTweet", methods=["POST"])
@login_required
def onNewTweet():
    user = request.form.get("user__name")
    parse_answer = request.form.get("full_text").split(" #")
    answer = parse_answer[0]

    return "Thanks!"


def queue_work(tweetId, twitterHandle, playURL):
    workQueueTable.create({"tweetId": tweetId, "twitterHandle": twitterHandle, "playURL":playURL, "completed": False, "attempts": 0 })

def get_all_queued_work():
    formula = AND(EQUAL(FIELD("completed"), to_airtable_value(0)), IF("{attempts} < 3", 1, 0))
    return workQueueTable.all(formula=formula)


def get_cached_result(playURL):
    formula = EQUAL(FIELD("playURL"), to_airtable_value(playURL))
    allRows = leaderboardTable.all(formula=formula)
    if len(allRows) == 0:
        return None

    return allRows[0]


def complete_queued_work(recordId):
    workQueueTable.update(recordId, {"completed": True})


def increment_attempts_queued_work(recordId):
    row = workQueueTable.get(recordId)
    attempts = row["fields"]["attempts"] + 1
    workQueueTable.update(recordId, {"attempts" : attempts})
    return attempts


def add_leaderboard_entry(playerName, scoringPayload):
    expression = scoringPayload["expression"]
    gameplayUrl = scoringPayload["gameplay"]
    level = scoringPayload["level"]
    charCount = scoringPayload["charCount"]
    playURL = scoringPayload["playURL"]
    time = scoringPayload["time"]
    leaderboardTable.create({"expression": expression, "time":time, "level":level, "playURL": playURL, "charCount":charCount, "player":playerName, "gameplay":gameplayUrl})

def notify_user_unknown_error(playerName, tweetId):
    print("We should notify user %s of error re: tweet with ID: %s" % (playerName, tweetId))


def notify_user_highscore_already_exists(playerName, tweetId, cachedResult):
    print("We should notify user %s of duplicate high score re: tweet with ID: %s" % (playerName, tweetId))


def do_scoring(workRow):
    recordId = workRow["id"]
    levelUrl = workRow["fields"]["playURL"]
    playerName = workRow["fields"]["twitterHandle"]
    tweetId = workRow["fields"]["tweetId"]
    print("Scoring for player: %s" % playerName)

    cachedResult = get_cached_result(levelUrl)
    if cachedResult is not None:
        complete_queued_work(recordId)
        notify_user_highscore_already_exists(playerName, tweetId, cachedResult)
        return
    response = requests.post(url=scoring_service_uri, json={"level": levelUrl}, verify=False)
    attempts = increment_attempts_queued_work(recordId)

    if response.status_code == 200:
        print("Successfully completed work: %s" % recordId)
        add_leaderboard_entry(playerName, json.loads(response.text))
        complete_queued_work(recordId)
    elif attempts >= 3:
        notify_user_unknown_error(playerName, tweetId)
        complete_queued_work(recordId)


def process_work_queue():
    try:
        print("Processing work queue")
        queued_work = get_all_queued_work()
        for workRow in queued_work:
            try:
                do_scoring(workRow)
            except Exception as e:
                print("Failed scoring (likely couldn't connect to scoring host) for: %s" % workRow["fields"]["tweetId"])
        print("Work queue processed!")
    except Exception as e:
        print("Exception: %s" % e)

def get_random_level():
    randomLevels = [
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAZQBkB5ABQFEB9EovASQBVaA1MkZAZwEMwBTAEwKcALjwAeuccgDmAe04AbdlBQBXefIC+QA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAZQBkB5ABQFEB9AOTIHEBBAFQEkA1MkZAZwEMwBTACYEeAF34APXAFopyAOYB7HgBsuUFAFcVKgL5A===",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAZQBkB5ABQFEB9PAFTLPICURkBnAQzAFMATAtgF04APXAFoATCOQBzAPZsANiygoArgoUBfIA",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAZQBkB5ABQFEB9ACQEkBxKsgJRGQGcBDMAUwBMCOAF24APXAFoRAagAcrEAHMA9hwA2bKCgCuq1QF8gA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAZQBkB5ABQFEB9YgdTICURkBnAQzAFMATAlgF3YAeuALQDhAZkYgA5gHsWAGyZQUAVwUKAvkA",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAZQBkB5ABQFEB9PACQEEDiB1MgJRGQGcBDMAUwBMCXAC68AHrgA6kgGYAnLhmABaMQF9gAFjXsQAcwD2XADYcoKAK7HjaoA==",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAZQBkB5ABQFEB9PAYQEEDKAVAJVoDlDbGyRkBnAIZgApgBMCAgC7CAHrgC0AHUUAzAE4CMwGQF9gAZh3yArLxABzAPYCANnygoArjZs6gA===",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRABQEEAlAgIQHkAZAgfQDkBRAcQIBUGRkBnAQzAFMAJpR4AXfgA9cEgHoAmTiADmAex4AbLlBQBXdeoC+QA===",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRABQEEAlAgIQHkAZAgfQBUSA5AZWroFEaBNEZAZwCGYAKYATSgIAuwgB64ZAPQBMAWgDMvEAHMA9gIA2fKCgCu+/QF8gA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRABQEEAlAgIQHkAZAgfQGUBhAygURoE0RkBnAQzACmAE0q8ALgIAeuADoyANgIBmYgBRylAJ14ZgkgL7AAHPrma0AcwAWYgJQA9AExcQFgPa953KCgCu8+X0gA",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRABQEEAlAgIQHkAZAgfQBUSA5AZWroFEaANEZAZwCGYAKYATSgIAuwgB64AFDIDUAZgCUAPQBMvEAHMA9gIA2fKCgCux4wF8gA",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRABQEEAlAgIQHkAZAgfQGUBhAygURoA0RkBnAQzACmAE0q8ALgIAeuSQD0ATFxABzAPa8ANtygoArho0BfIA==",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRABQEEAlAgIQHkAZAgfQGUBhAygURoA0RkBnAQzACmAE0q8ALgIAeuADoyANgIBmYgBRylAJ14ZgkgL7AATPrma0AcwAWYgJQA9I1xAWA9r3ncoKAK7z5+kA==",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRABQEEAlAgIQHkAZAgfQBUSA5AZWroFEaANGgTRGQBnAIZgApgBNKwgC5iAHrgAU8gNQBmAJQA9AEwBaACwCQAcwD2wgDaCoKAK5WrAXyA",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRABQEEAlAgIQHkAZAgfQBUSA5AZWroFEbmBhAyzgBohkAZwCGYAKYATSmIAukgB64AOqoA2kgGbyAFOu0AnMRmBKA1AGYAvsABMN9UbQBzABbyAlAD17wkFcAezENESgUAFcNDRsgA",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRABQEEAlAgIQHkAZAgfQBUSA5AZWroFEbmBhAyzgBo0AmiGQBnAIZgApgBNKkgC4yAHrgA6GgDYyAZkoAUWvQCdJGYKoC0ADgC+wAEz2tptAHMAFkoCUAPSdrABYxEA8Ae0ltcSgUAFdtbXsgA",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQEkBZAUQH0BhAeQDkBlPAQRrxGQGcBDMAUwBMAMpwAuPAB64AbAFphbEAHMA9pwA27KCgCuq1QF8gA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQEkBZAUQH0BhAeSoBkRkBnAQzAFMATW5gFzYA9cAHSEAbNgDMeACn4BqAMwiATmgDmACx4BKERg4B7HiPFTpPALQiJy5hmAKAvsAAsjleq3aGINQeaijFAoAK6ioo5AA===",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQEkBZAUQH0AFAQQCUqAhAeQBkqy86A5AZVb3IAaIZAGcAhmACmAE2ZiALpIAeuADqqANpIBm8gBRKAtPIDUAZnUAnNAHMAFvICUAPQBMwkDYD2YjSKgoAK4aGgC+QA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQEkBZAUQH0AFAQQCUqAhAeQBkqy86A5AZVb3IAaZAJohkAZwCGYAKYATZpIAuMgB64AOhoA2MgGZKAFKoC0SrQCc0AcwAWSgJQA9AEwBqJWJDWA9pO3iUCgArtraAL5AA==",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQEkBZAUQH0AFAQQCUqAhAeQBkqy86A5AZVb3IAaZAJpluAYSrNywkMgDOAQzABTACbNFAFxUAPXAB0DAGxUAzLQApdAWi1GATmgDmACy0BKAHoAmANRGZg6KGMBaAL7APuFyIM4A9orG8lAoAK7GxuFAA==",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQEkBZAUQH0AJAQQCUAREZAZwEMwBTAEwBkWAXdgA9cAHREAbdgDM+ACjFSATiwzBBAagAs6sUzQoxkmfJFKVwPgF9gADktjFaAOYALPgEoxGTgHs+8aGs7B2c3dwA9TQBaGyivHyZDaTk+L19/WABWENcPJOMFZVV4a3gdCWTZDQB2LJyw8IAmexFHXPdys1UrYGbGECcfFnEmKBQAV3FxSyA==",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAZQEkA5EZAZwEMwBTAEwBkKAXagD1wFoAdLstFHgBtqAMyYAKVjwBOaAOYALJgEpSIOQHsKgslBQBXQYIC+QA",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAYQHkBlEZAZwEMwBTAEwBkKAXagD1wDYBqAHR4wD2ZPgBtqAMyYAKVnwBOaAOYALJgEpSIRQIoiyUFAFcRIgL5A==",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAZQEkA5AfTwGEBBAGQFESANEZAZwEMwBTAE2rYBdOAD1wAdUSzQpxAG04AzfgApx8gE5sMwIQF9gAJh3i1aAOYALfgEpmIUwHs2MllBQBXGTJ1A==",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAZQEkA5AfQBUAlAQSLwBkqyBREgDRGQGcBDMAUwAmdbgBc+AD1wAdKZzQoZAGz4AzEQApxAWhkqATtwzAADmgC+wAExmZetAHMAFiICUHEPYD23RZygoAV0VFMyA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAYQHkBlAfSLwEEAZAURIA0SBNEZAZwEMwBTAEyo4AXbgA9cAWgA6kjAHs20gDbcAZoIAU0lQCcOGYCIC+wAEyHp2tAHMAFoICUrEFdkdFbKCgCuixYaA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAZQEkA5AfTwGEBBAGQFESANEgTRGQGcBDMAUwBNqnAC48AHrgC0AZgA6M9mhRyANjwBmQgBRy1AJ04ZgogL7ApxubrQBzABZCAlGxDWA9p2XsoKAK7LlxkA==",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAZQEkA5AfQBUAlAQSLwBkqyBREgDRIE0RkBnAQzABTACZ0+AF0EAPXAB1ZGAPY95AG0EAzcQAopAWnjyATmgDmAC3EBKANQAWbiFOK+qnlBQBXVaoC+QA",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQEkBZAUQH0AFAQQCUqAhAeQBkqyaCBlEmkZAZwCGYAKYATZoIAuIgB64AOgoA2IgGZSAFErUAnQRmCyAvsAAsxpbrQBzABZSAlAD0ATAGopAWgDMfEDYA9oLK/FAoAK7KysZAA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQEkBZAUQH0BlAgOTLwCUBBaigGUb3IA0RkBnAIZgApgBNWAgC7CAHrgA68vmhSKANsIBmkgBSLNAJwEZgMgNTwATAF9gNgLSTFBtAHMAFpICUvEK4D2Amp8UCgArmpq1kA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQEkBZAUQH0BlAgOTJIoGEBBAGSbwHkAlEZAZwCGYAKYATFgIAuwgB64AzAGoALAB1VfNCnUAbYQDNJACnX6ATgIzAATDIC+wAA5o7AWlMWrkh9bvqzaADmABaSAJSKcsiBAPYCOnxQKACuOjp2QA===",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQEkBZAUQH0AFAQQCUqAhAeQBkqyA5EgcSr0ZpDIAzgEMwAUwAmzEQBdxAD1wAdZQBtxAM1kAKVZoBOIjMAUBfYAFYzqg2gDmAC1kBKAHoAmVRkkB7Waoa2nrKhsbAshYeZgC08LYOzi5xgiD2viJqQlAoAK5qamZAA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQEkBZAUQH0BlAgOTJIoGEBBAGSbwHkAlMjxglm05cQyAM4BDMAFMAJiwkAXaQA9cAWgBMAHW0YA9mN0AbaQDNFAChXqA7LrFoUJ81d1mAThIzBFAX2AAZj9dDzQAcwALRQBKUIjomIBqFST4URBw/QljMSgUAFdjYz8gA==",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQEkBZAUQH0BlAgOTJIA0AFAQWoBEaBxMgeQoGECAGSHM8PAEohkAZwCGYAKYATIXIAuigB64AtAB19GAPYzDAG0UAzdQAotB/TLQoL1u+sMAnNAHMAFuoAlIYYysae+lZechjA6gC+wABMCd5+gUHSIL7GcuYyUCgArubmCUA==",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQEkBZAUQH0BlAgOTIAUBBAJQYCEB5AGQbPYoGECnbnnZMQyAM4BDMAFMAJp2kAXOQA9cAHS0AbOQDMVACh0GATtIzB1AX2AAWWwFoATDsloUO/UeNwAdhUdczQAcwALFQBKEPCo6IA9VwkQMIB7aV1JKBQAV11dWyA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAGQHkBxASQGUAVEgYRGQGcBDMAUwBM9GAXFgD1wA6AgGYAnRhmDwAvlIDUQgDYthXABS8AtACYhotAHMAFlwCUAPW3S6IAwHtGi+lBQBXRYulA===",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAGQHkBxASQGUAVEgYQH0AlAUQPoBFH6RkBnAQzACmAEzy8ALgIAeuADoyAZgCdeGYACYAvsHgBqOQBsB8sQApJOgBxzFaAOYALMQEoAepr0Llq+Ft0GjppIAtPDWdo6umlwgtgD2vPrcUCgArvr6GkA==",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAGQHkBxASQGUAVEgYQH0ApAVQFkAFEZAZwEMwBTACZ5uAFz4APXAB0pAMwBO3DMAAsAX2DwA1DIA2fWSIAU4rQA4Z8tAHMAFiICUAPQBManXMXL4G7eNfuMgpKwG6aHvqGJgC0FlJWdo4BHCDWAPbcupxQKACuurpqQA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAGQHkBxASQGUAVEgYQH0AFAQQCVGAhAvRkZAZwEMwAUwAmefgBchAD1wAdOQDMATvwzB4AJgC+GgNTSAejr0KANkMUSAFApVrg03QBZtC5WgDmACwkBKYx4QTwB7fjNeKBQAVzMzbSA==",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAGQHkBxASQGUAVEgYQH0KAlAQQCkBRaighgTRGQDOAQzABTACZ4hAF1EAPXAB1FAG1EAzaQApl6gE5CMwOQF9gAZhPK9aAOYALaQEoAegCYA1LoNGALGa14Dzl3JxN+EFsAeyEVASgUAFcVFRMgA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQHUB5AfQCEBRAZQEkARCkZAZwEMwBTAEwBlWAXDgA9cAHVEAbDgDN+ACnHSATqwzAhAWgAcAX2AAmHeKVoA5gAt+ASgB6+jfGgBqfkxCmA9qwnMoKAK4SEjpAA=",
        "https://sinerider.hackclub.dev/?N4IgbiBcAMB0CMAaEA7AlgYwNZRAFQHUB5AfQCEBRAGSIJGQGcBDMAUwBMqmAXVgD1wAdQQBtWAM24AKYeIBOTDMD4BaeABYAvsC0BqbsLloA5gAtuASgB6AJhUAOeiGMB7JiIZQUAVxEjNQA==="
    ]
    return randomLevels[random.randint(0, len(randomLevels)-1)]


def add_test_data(numTests):
    print("Adding test data")
    for index in range(numTests):
        queue_work("tweet_id_%d" % random.randint(0, 1000000), "TwitterUser%d" % random.randint(0, 1000000), get_random_level())

print("This resolved")
if __name__ == "__main__":
    print("Main running")
    polling.poll(process_work_queue, step=10, poll_forever=True)
    app.run(port=3000, debug=True, use_reloader=False)
