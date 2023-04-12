import os
import json
import random
import sys
import traceback

import tweepy
import requests
import polling
from flask import Flask, request, redirect
from pyairtable import Table
from dotenv import load_dotenv
from auth import login_required
from pyairtable.formulas import EQUAL, AND, IF, FIELD, to_airtable_value
import threading

app = Flask(__name__)
app.secret_key = os.urandom(50)

load_dotenv()

airtable_api_key = os.environ["AIRTABLE_API_KEY"]
airtable_base_id = os.environ["AIRTABLE_BASE_ID"]
consumer_secret = os.environ["CONSUMER_SECRET"]
consumer_key = os.environ["CONSUMER_KEY"]
redirect_uri = os.environ["REDIRECT_URI"]
scoring_service_uri = os.environ["SINERIDER_SCORING_SERVICE"]

auth_url = "https://twitter.com/i/oauth2/authorize"
token_url = "https://api.twitter.com/2/oauth2/token"
scopes = ["tweet.read", "users.read", "tweet.write", "offline.access"]

bearer_token_config_key = "reccSXSMsSnxvFVdM" # note - this is not a secret
refresh_token_config_key = "reca8u53hlSnNLxTn" # note - this is not a secret

workQueueTable = Table(airtable_api_key, airtable_base_id, "TwitterWorkQueue")
leaderboardTable = Table(airtable_api_key, airtable_base_id, "Leaderboard")
authTable = Table(airtable_api_key, airtable_base_id, "TwitterAuth")

AUTHORIZE_MANUALLY = False

class MyOAuth2UserHandler(tweepy.OAuth2UserHandler):
    def refresh_token(self, refresh_token):
        new_token = super().refresh_token(
                "https://api.twitter.com/2/oauth2/token",
                refresh_token=refresh_token,
                body=f"grant_type=refresh_token&client_id={self.client_id}",
            )
        return new_token

def get_bearer_token():
    oauth2_user_handler = tweepy.OAuth2UserHandler(
        client_id=consumer_key,
        redirect_uri=redirect_uri,
        scope=scopes,
        client_secret=consumer_secret
    )
    authorization_response_url = input("Please navigate to the url: " + oauth2_user_handler.get_authorization_url())
    access_token = oauth2_user_handler.fetch_token(
        authorization_response_url
    )
    print(access_token)
    return access_token


def set_config(key, value):
    authTable.update(key, {"value": value})

def post_tweet(bearer_token, msg, response_tweet_id=None):
    print("Posting tweet with content: %s" % (msg))
    client = tweepy.Client(bearer_token)

    if response_tweet_id is None:
        return client.create_tweet(text=msg, user_auth=False)
    else:
        return client.create_tweet(text=msg, user_auth=False, in_reply_to_tweet_id=response_tweet_id)

@app.route("/onNewTweet", methods=["POST"])
@login_required
def on_new_tweet():
    user_name = request.form.get("user__name")
    id = request.form.get("id")
    user_id = request.form.get("user__id")
    in_reply_to_status_id = request.form.get("in_reply_to_status_id")

    if user_name == "Sinerider bot" or user_id == '1614221762719367168':
        print("Detected bot tweet, ignoring...")
        return "We don't respond to bots"

    text_parts = request.form.get("full_text").split("#sinerider ")
    if len(text_parts) != 2:
        print("Cannot parse tweet")
        return "weird!"

    play_url = "https://sinerider.hackclub.dev/?" + text_parts[1]

    queue_work(id, user_name, play_url)
    return "Thanks!"


def queue_work(tweetId, twitterHandle, playURL):
    workQueueTable.create({"tweetId": tweetId, "twitterHandle": twitterHandle, "playURL":playURL, "completed": False, "attempts": 0 })

def get_all_queued_work():
    formula = AND(EQUAL(FIELD("completed"), to_airtable_value(0)), IF("{attempts} < 3", 1, 0))
    return workQueueTable.all(formula=formula)


def get_one_row(table, fieldToMatch, value):
    formula = EQUAL(FIELD(fieldToMatch), to_airtable_value(value))
    try:
        allRows = table.all(formula=formula)
        if len(allRows) == 0:
            return None

        return allRows[0]
    except:
        print("Shouldn't have happened!")
        return ""

def get_cached_result(playURL):
    return get_one_row(leaderboardTable, "playURL", playURL)

def complete_queued_work(recordId):
    workQueueTable.update(recordId, {"completed": True})


def increment_attempts_queued_work(recordId):
    row = workQueueTable.get(recordId)
    attempts = row["fields"]["attempts"] + 1
    workQueueTable.update(recordId, {"attempts": attempts})
    return attempts


def add_leaderboard_entry(playerName, scoringPayload):
    print("adding leaderboard entry")
    expression = scoringPayload["expression"]
    gameplayUrl = scoringPayload["gameplay"]
    level = scoringPayload["level"]
    charCount = scoringPayload["charCount"]
    playURL = scoringPayload["playURL"]
    time = scoringPayload["time"]
    leaderboardTable.create({"expression": expression, "time":time, "level":level, "playURL": playURL, "charCount":charCount, "player":playerName, "gameplay":gameplayUrl})

def notify_user_unknown_error(playerName, tweetId):
    print("Notify user %s of unknown error re: tweet with ID: %s" % (playerName, tweetId))
    error_message = "Sorry, I encountered an error scoring that submission :("
    post_tweet(get_config(bearer_token_config_key, "<unknown>"), error_message, tweetId)


def notify_user_highscore_already_exists(playerName, tweetId, cachedResult):
    print("We should notify user %s of duplicate high score re: tweet with ID: %s" % (playerName, tweetId))
    error_message = "Sorry, someone already submitted that solution to the leaderboards!"
    post_tweet(get_config(bearer_token_config_key, "<unknown>"), error_message, tweetId)


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
        score_data = json.loads(response.text)
        add_leaderboard_entry(playerName, score_data)

        try:
            if "time" not in score_data or score_data["time"] is None:
                msg = "Sorry, that submission takes longer than 30 seconds to evaluate, and thus is disqualified :("
                post_tweet(get_config(bearer_token_config_key, "<unknown>"), msg, tweetId)
            else:
                msg = "Good job, you made it on the leaderboard for %s with a time of %f and a charCount of %d.  Check out this video of your run: %s" % (score_data["level"], score_data["time"], score_data["charCount"], score_data["gameplay"])
                post_tweet(get_config(bearer_token_config_key, "<unknown>"), msg, tweetId)
        except:
            print("Error posting tweet response...")
        complete_queued_work(recordId)
    elif attempts >= 3:
        print("Too many attempts, notifying user of error")
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
                traceback.print_exc()
                print("Failed scoring (likely couldn't connect to scoring host) for: %s" % workRow["fields"]["tweetId"])
                print(e)
        print("Work queue processed!")
        sys.stdout.flush()
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

def get_config(key, default):
    res = authTable.get(key)
    if "value" in res["fields"]:
        return res["fields"]["value"]
    else:
        return default


def refresh_auth_token():
    refresh_token = get_config(refresh_token_config_key, "<null>")
    auth = MyOAuth2UserHandler(
        client_id=consumer_key,
        redirect_uri=redirect_uri,
        scope=scopes,
        client_secret=consumer_secret
    )
    fullToken = auth.refresh_token(refresh_token)
    set_config(bearer_token_config_key, fullToken["access_token"])
    set_config(refresh_token_config_key, fullToken["refresh_token"])
    print("refreshed twitter, auth token - should have 2 more hours")

def run_server():
    app.run(port=8080, debug=True, use_reloader=False)

def run_polling():
    polling.poll(process_work_queue, step=10, poll_forever=True)

def refresh_token_polling():
    polling.poll(refresh_auth_token, step=5*60, poll_forever=True)

def get_auth_token():
    if AUTHORIZE_MANUALLY:
        fullToken = get_bearer_token()
        set_config(bearer_token_config_key, fullToken["access_token"])
        set_config(refresh_token_config_key, fullToken["refresh_token"])
        return
    else:
        return get_config(bearer_token_config_key, "<null>")

if "PROC_TYPE" not in os.environ:
    print("PROC_TYPE=null (probably running locally)")
    threading.Thread(target=run_server).start()
    threading.Thread(target=run_polling).start()
    threading.Thread(target=refresh_token_polling).start()
elif os.environ["PROC_TYPE"] == "web":
    print("PROC_TYPE=web, starting server...")
    threading.Thread(target=run_server).start()
elif os.environ["PROC_TYPE"] == "worker":
    print("PROC_TYPE=worker, starting polling...")
    threading.Thread(target=run_polling).start()
    threading.Thread(target=refresh_token_polling).start()
else:
    print("INVALID WORKER TYPE")

