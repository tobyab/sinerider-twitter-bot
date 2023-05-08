import os
import json
import random
import re
import sys
import traceback
import uuid
import tweepy
import requests
import polling
from flask import Flask, request, Response
from pyairtable import Table
from dotenv import load_dotenv
from auth import login_required
from pyairtable.formulas import EQUAL, AND, IF, FIELD, to_airtable_value
import threading
import lzstring

app = Flask(__name__)
app.secret_key = os.urandom(50)

load_dotenv()

airtable_api_key = os.environ["AIRTABLE_API_KEY"]
airtable_base_id = os.environ["AIRTABLE_BASE_ID"]
v11_consumer_key = os.environ["V11_CONSUMER_KEY"]
v11_consumer_secret = os.environ["V11_CONSUMER_SECRET"]
v11_access_token = os.environ["V11_ACCESS_TOKEN"]
v11_access_token_secret = os.environ["V11_ACCESS_TOKEN_SECRET"]

v20_consumer_key = os.environ["V20_CONSUMER_KEY"]
v20_consumer_secret = os.environ["V20_CONSUMER_SECRET"]
v20_scopes = ["tweet.read", "users.read", "tweet.write", "offline.access"]

redirect_uri = os.environ["REDIRECT_URI"]
scoring_service_uri = os.environ["SINERIDER_SCORING_SERVICE"]
leaderboard_uri = os.environ["LEADERBOARD_URI"]

workQueueTable = Table(airtable_api_key, airtable_base_id, "TwitterWorkQueue")
leaderboardTable = Table(airtable_api_key, airtable_base_id, "Leaderboard")
configTable = Table(airtable_api_key, airtable_base_id, "Config")
puzzleTable = Table(airtable_api_key, airtable_base_id, "Puzzles")
AUTHORIZE_MANUALLY = False

twitter_v11_api = tweepy.API(tweepy.OAuth1UserHandler(v11_consumer_key, v11_consumer_secret, v11_access_token, v11_access_token_secret))

def get_twitter_v11():
    return twitter_v11_api

def get_twitter_v20():
    bearer_token = get_config("bearer_token", "<unknown>")
    return tweepy.Client(bearer_token)

def set_config(key, value):
    existing_config = get_one_row(configTable, "config_name", key)
    if existing_config is None:
        configTable.create({"config_name": key, "value":value})
    else:
        configTable.update(existing_config["id"], {"value": value})


def post_tweet(msg, response_tweet_id=None, media_ids=None):
    print("Posting tweet with content: %s" % (msg))
    if response_tweet_id is None:
        return get_twitter_v20().create_tweet(text=msg, user_auth=False, media_ids=media_ids)
    else:
        return get_twitter_v20().create_tweet(text=msg, user_auth=False, in_reply_to_tweet_id=response_tweet_id, media_ids=media_ids)

@app.route("/publishPuzzle", methods=["POST"])
@login_required
def on_publish_puzzle():
    publish_info = request.args.get("publishingInfo")
    lztranscoder = lzstring.LZString()
    json_str = lztranscoder.decompressFromBase64(publish_info)
    exploded_publish_info = json.loads(json_str)
    puzzle_id = exploded_publish_info["id"]
    puzzle_title = exploded_publish_info["puzzleTitle"]
    puzzle_description = exploded_publish_info["puzzleDescription"]
    puzzle_url = exploded_publish_info["puzzleURL"]

    puzzle_post_text = "%s - %s %s" % (puzzle_title, puzzle_description, puzzle_url)
    print("Publishing puzzle on twitter: %s" % (puzzle_post_text))

    try:
        response = post_tweet(puzzle_post_text)
        tweet_id = response.data["id"]
        print("Successfully published puzzle (tweet id: %s)" % (tweet_id))
        set_config("twitter_%s" % (puzzle_id), tweet_id)
        return Response(status=200)
    except Exception as e:
        error_message = str(e).replace('\n', ' ').replace('\r', '')
        print("Failed to publish puzzle: message=%s" % (error_message))
        resp = Response("{'message':'%s'}" % error_message, status=500, mimetype='application/json')
        return resp


@app.route("/onNewTweet", methods=["POST"])
@login_required
def on_new_tweet():
    user_name = request.form.get("user__name")
    tweet_id = request.form.get("id")
    twitter_user_id = request.form.get("user__id")

    if user_name == "Sinerider bot" or twitter_user_id == '1614221762719367168':
        print("Detected bot tweet, ignoring...")
        return "We don't respond to bots"

    match = re.search(r"#sinerider\s(?P<puzzle_id>puzzle_\d+)\s(?P<equation>.*)", request.form.get("full_text"))
    if match:
        puzzle_number = match.group("puzzle_id")
        equation = match.group("equation")
        print("puzzle: ", puzzle_number)
        print("equation: ", equation)

    else:
        print("Cannot parse tweet")
        return "We can't parse this tweet!"

    if validate_puzzle_id(puzzle_number) == False:
        print("We should notify user %s of invalid puzzle id re: tweet with ID: %s" % (user_name, id))
        error_message = "Sorry, I don't know what puzzle you're talking about..."
        post_tweet(error_message, tweet_id)
        return "weird!"

    queue_work(tweet_id, user_name, puzzle_number, equation)

    return "Thanks!"


def validate_puzzle_id(puzzle_id):
    # Note - we need to ensure the puzzle exists...
    try:
        puzzle = get_one_row(puzzleTable, "id", puzzle_id)
        if puzzle is not None:
            return True
    except:
        print("error validating puzzle id, probably failed to query airtable")
    return False


def queue_work(tweetId, twitterHandle, puzzleId, expression):
    workQueueTable.create({"tweetId": tweetId, "twitterHandle": twitterHandle, "puzzleId":puzzleId, "expression":expression, "completed": False, "attempts": 0})


def get_all_queued_work():
    formula = AND(EQUAL(FIELD("completed"), to_airtable_value(0)), IF("{attempts} < 3", 1, 0))
    return workQueueTable.all(formula=formula)


def get_one_row(table, fieldToMatch, value):
    formula = EQUAL(FIELD(fieldToMatch), to_airtable_value(value))
    try:
        all_rows = table.all(formula=formula)
        if len(all_rows) == 0:
            return None

        return all_rows[0]
    except:
        print("Shouldn't have happened!")
        return ""


def complete_queued_work(tweet_id):
    row = get_one_row(workQueueTable, "tweetId", tweet_id)
    workQueueTable.update(row["id"], {"completed": True})


def increment_attempts_queued_work(tweet_id):
    row = get_one_row(workQueueTable, "tweetId", tweet_id)
    attempts = row["fields"]["attempts"] + 1
    workQueueTable.update(row["id"], {"attempts": attempts})
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
    post_tweet(error_message, tweetId)


def notify_user_highscore_already_exists(playerName, tweetId, cachedResult):
    print("We should notify user %s of duplicate high score re: tweet with ID: %s" % (playerName, tweetId))
    error_message = "Sorry, someone's already submitted that solution to the leaderboards — try again with a different answer!"
    media = None
    if "fields" in cachedResult and "gameplay" in cachedResult["fields"] and len(cachedResult["fields"]["gameplay"]) > 0:
        try:
            media = upload_media_to_twitter(cachedResult["fields"]["gameplay"], "video/mp4")
        except Exception as e:
            media = None
    post_tweet(error_message, tweetId, media)


def notify_user_invalid_puzzle(player_name, tweet_id):
    print("We should notify user %s of invalid puzzle on tweet with ID: %s" % (player_name, tweet_id))
    error_message = "I'm terribly sorry, but I'm not aware of a puzzle with that name!"
    post_tweet(error_message, tweet_id)


def upload_media_to_twitter(media_url, file_type):
    filename = "./%s-gameplay.%s" % (uuid.uuid4(), file_type.split("/")[1])
    try:
        r = requests.get(media_url, allow_redirects=True)
        file = open(filename, 'wb')
        file.write(r.content)
        file.flush()
        file.close()
        media = get_twitter_v11().chunked_upload(filename, file_type=file_type, additional_owners=[1614221762719367168])
        return [media.media_id_string]
    except Exception as e:
        print(e)
    finally:
        if os.path.exists(filename):
            os.remove(filename)
    return None


def do_scoring(workRow):
    # Note - we need to load the puzzle URL from the Puzzles table
    puzzle_id = workRow["fields"]["puzzleId"]
    expression = workRow["fields"]["expression"]
    player_name = workRow["fields"]["twitterHandle"]
    tweet_id = workRow["fields"]["tweetId"]
    print("Scoring for player: %s" % player_name)

    caching_key = "%s %s" % (puzzle_id, expression)
    puzzle_data = get_one_row(puzzleTable, "id", puzzle_id)

    # Validate that the puzzle exists
    if puzzle_data is None:
        complete_queued_work(tweet_id)
        notify_user_invalid_puzzle(player_name, tweet_id)
        return

    # Construct the proper level URL based on the puzzle id + expression by using the puzzle data URL
    # and decoding its contents, and then inserting the expression into it
    puzzle_url = puzzle_data["fields"]["puzzleURL"]
    url_parts = puzzle_url.partition("?")
    url_prefix = url_parts[0]
    lzstr_base64_encoded_data = url_parts[2]
    lztranscoder = lzstring.LZString()
    exploded_puzzle_data = json.loads(lztranscoder.decompressFromBase64(lzstr_base64_encoded_data))
    exploded_puzzle_data["expressionOverride"] = expression

    responses = [
        "Grooooovy! You're on the leaderboard for %s with a time of %f (speedy!!) and a character count of %d! Also, we made you an *awesome* video of your run!\r\nCheck your spot on the leaderboards here: %s\r\nRemix their solution: %s",
        "Woohoo!! You're on the leaderboard for %s with a time of %f (vroom vroom!) and a character count of %d! Check out this super cool video of your run!\r\nCheck your spot on the leaderboards here: %s\r\nRemix their solution: %s",
        "🥳🥳🥳 You're on the %s leaderboard with a super speedy time of %f and a character count of %d! We even made this groovy video of your run!\r\nCheck your spot on the leaderboards here: %s\r\nRemix their solution: %s",
        "Cowabunga! You've made it onto the %s leaderboard! You got an unbelievably fast time of %f (WOW!) and a character count of %d! There's even a super cool video of your run!\r\nCheck your spot on the leaderboards here: %s\r\nRemix their solution: %s",
    ]

    # test code
    # url_prefix = "http://localhost:5500"
    # test code
    
    submission_url = url_prefix + "?" + lztranscoder.compressToBase64(json.dumps(exploded_puzzle_data))

    print("Using submission URL: " + submission_url)

    # See if we have already scored this submission
    cached_result = get_one_row(leaderboardTable, "playURL", submission_url)
    if cached_result is not None:
        complete_queued_work(tweet_id)
        notify_user_highscore_already_exists(player_name, tweet_id, cached_result)
        return

    # Do the work of scoring
    attempts = increment_attempts_queued_work(tweet_id)
    response = requests.post(url=scoring_service_uri, json={"level": submission_url}, verify=False)

    if response.status_code == 200:
        print("Successfully completed work: %s" % tweet_id)
        score_data = json.loads(response.text)
        add_leaderboard_entry(player_name, score_data)

        try:
            if "time" not in score_data or score_data["time"] is None:
                msg = "Sorry, that submission takes longer than 30 seconds to evaluate, so we had to disqualify it. :( Try again with a new solution!"
                post_tweet(msg, tweet_id)
            else:
                msg = random.choice(responses) % (score_data["level"], score_data["time"], score_data["charCount"], leaderboard_uri, score_data["playURL"])
                post_tweet(msg, tweet_id, upload_media_to_twitter(score_data["gameplay"], "video/mp4"))
        except Exception as e:
            print("Error posting tweet response...")
        complete_queued_work(tweet_id)
    elif attempts >= 3:
        print("Too many attempts, notifying user of error")
        notify_user_unknown_error(player_name, tweet_id)
        complete_queued_work(tweet_id)


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
    except Exception as e:
        print("Exception: %s" % e)
    print("Work queue end")
    sys.stdout.flush()


def get_config_id(key):
    return get_one_row(configTable, key, None) is not None


def get_config(key, default):
    val = get_one_row(configTable, "config_name", key)
    if val is None:
        return default
    return val["fields"]["value"]

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
        client_id=v20_consumer_key,
        redirect_uri=redirect_uri,
        scope=v20_scopes,
        client_secret=v20_consumer_secret
    )
    authorization_response_url = input("Please navigate to the url: " + oauth2_user_handler.get_authorization_url())
    access_token = oauth2_user_handler.fetch_token(
        authorization_response_url
    )
    print(access_token)
    return access_token

def refresh_auth_token():
    refresh_token = get_config("refresh_token", "<null>")
    auth = MyOAuth2UserHandler(
        client_id=v20_consumer_key,
        redirect_uri=redirect_uri,
        scope=v20_scopes,
        client_secret=v20_consumer_secret
    )
    fullToken = auth.refresh_token(refresh_token)
    set_config("bearer_token", fullToken["access_token"])
    set_config("refresh_token", fullToken["refresh_token"])
    print("refreshed twitter, auth token - should have 2 more hours")

def refresh_token_polling():
    polling.poll(refresh_auth_token, step=5*60, poll_forever=True)

def get_auth_token():
    if AUTHORIZE_MANUALLY:
        fullToken = get_bearer_token()
        set_config("bearer_token", fullToken["access_token"])
        set_config("refresh_token", fullToken["refresh_token"])
        return
    else:
        return get_config("bearer_token", "<null>")

if AUTHORIZE_MANUALLY:
    get_auth_token()


def run_server():
    app.run(port=8080, debug=True, use_reloader=False)


def run_polling():
    polling.poll(process_work_queue, step=10, poll_forever=True)


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
