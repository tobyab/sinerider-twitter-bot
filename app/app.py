import os
import json
import random
import sys
import asyncio
import traceback
import time

import requests
import polling
from flask import Flask, request, Response, g
from dotenv import load_dotenv
from flask_auth import login_required
import threading
import lzstring
from persistence import Persistence
from twitter import TwitterClient
from metrics import metrics

app = Flask(__name__)
app.secret_key = os.urandom(50)

load_dotenv()

AUTHORIZE_MANUALLY = False
TESTING = False

scoring_service_uri = os.environ["SINERIDER_SCORING_SERVICE"]
leaderboard_uri = os.environ["LEADERBOARD_URI"]
persistence = Persistence(os.environ["AIRTABLE_API_KEY"], os.environ["AIRTABLE_BASE_ID"])
twitter_client = TwitterClient(persistence, json.loads(os.environ["TWITTER_CREDENTIALS_JSON"]),
                               os.environ["REDIRECT_URI"], TESTING)


@app.before_request
def get_metrics():
    g.start = time.time()
    g.method = request.method
    g.url = request.url

@app.after_request
def log_metrics(response: Response):
    diff = time.time() - g.start
    stat = (g.method + '/' + g.url.split('/')[3]).lower().replace("/", "_")
    http_code = response.status_code
    timing_stat_key = f'http.response.{stat}'
    code_stat_key = f'http.response.{stat}.{http_code}'
    metrics.timing(timing_stat_key, diff)
    metrics.incr(code_stat_key, 1)

    return response

@app.route("/publishPuzzle", methods=["POST"])
@login_required
def on_publish_puzzle():
    """ Endpoint that is called when we want to auto-publish a new puzzle on Twitter.
    :return: Response(200), or Response(500) + json error msg
    """
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
        # Note - we always want to publish these tweets using the primary bot account
        response = twitter_client.post_tweet(puzzle_post_text, use_primary_bot=True)
        tweet_id = response.data["id"]
        print("Successfully published puzzle (tweet id: %s)" % (tweet_id))
        persistence.set_config("twitter_%s" % (puzzle_id), tweet_id)
        return Response(status=200)
    except Exception as e:
        metrics.incr("error.publish_puzzle", 1)
        error_message = str(e).replace('\n', ' ').replace('\r', '')
        print("Failed to publish puzzle: message=%s" % (error_message))
        resp = Response("{'message':'%s'}" % error_message, status=500, mimetype='application/json')
        return resp


def notify_user_unknown_error(player_name, tweet_id):
    """ Tweet a generic error response to a submitter.
    :param playerName: The name of the player we're responding to
    :param tweetId: The ID of the tweet they submitted their answer with
    """
    error_message = "Sorry, I encountered an error scoring that submission :("
    metrics.incr("error.unknown", 1)
    twitter_client.post_tweet(error_message, tweet_id)


def notify_user_highscore_already_exists(player_name, tweet_id, cached_result):
    """ Tweet a response to a submitter saying that their high score already existed
    :param playerName: The name of the player we're responding to
    :param tweetId: The ID of the tweet they submitted their answer with
    :param cachedResult: The duplicate high-score record
    """
    print("We should notify user %s of duplicate high score re: tweet with ID: %s" % (player_name, tweet_id))
    error_message = "Sorry, someone already submitted that solution — try again with a different answer!"
    media = None
    if "fields" in cached_result and "gameplay" in cached_result["fields"] and len(
            cached_result["fields"]["gameplay"]) > 0:
        try:
            media = twitter_client.upload_media(cached_result["fields"]["gameplay"], "video/mp4")
        except Exception as e:
            media = None
    twitter_client.post_tweet(error_message, tweet_id, media)


def notify_user_invalid_puzzle(player_name, tweet_id):
    """ Tweet a response to a submitter saying that their submission was invalid, because the puzzle didn't exist
    :param player_name:
    :param tweet_id:
    """
    print("We should notify user %s of invalid puzzle on tweet with ID: %s" % (player_name, tweet_id))
    error_message = "I'm terribly sorry, but I'm not aware of a puzzle with that name!"
    metrics.incr("error.invalid_puzzle", 1)
    twitter_client.post_tweet(error_message, tweet_id)


async def do_scoring(work_row):
    """ Perform scoring for an item on the work queue
    :param workRow: A work item that needs to be scored and responded to
    :return: N/A
    """
    # Note - we need to load the puzzle URL from the Puzzles table
    puzzle_id = work_row["fields"]["puzzleId"]
    expression = work_row["fields"]["expression"]
    player_name = work_row["fields"]["twitterHandle"]
    tweet_id = work_row["fields"]["tweetId"]

    existing_tweetId = persistence.get_tweetId_data(tweet_id)

    if existing_tweetId:
        # if tweet_id already exists in the airtable
        notify_user_invalid_puzzle(player_name, tweet_id)
        return

    # Keep track of how many times we've attempted to score this work item
    attempts = persistence.increment_attempts_queued_work(tweet_id)

    print("[Attempt %d] Scoring tweet: %s user: %s puzzle_id: %s expression: \"%s\"" % (
    attempts, tweet_id, player_name, puzzle_id, expression))

    puzzle_data = persistence.get_puzzle_data(puzzle_id)

    # Validate that the puzzle exists
    if puzzle_data is None:
        persistence.complete_queued_work(tweet_id)
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
        "Grooooovy! You're on the leaderboard for %s with a time of %f (speedy!!) and a character count of %d! Also, we made you an *awesome* video of your run!\r\nCheck your spot on the leaderboards here: %s",
        "Woohoo!! You're on the leaderboard for %s with a time of %f (vroom vroom!) and a character count of %d! Check out this super cool video of your run!\r\nCheck your spot on the leaderboards here: %s",
        "🥳🥳🥳 You're on the %s leaderboard with a super speedy time of %f and a character count of %d! We even made this groovy video of your run!\r\nCheck your spot on the leaderboards here: %s",
        "Cowabunga! You've made it onto the %s leaderboard! You got an unbelievably fast time of %f (WOW!) and a character count of %d! There's even a super cool video of your run!\r\nCheck your spot on the leaderboards here: %s",
    ]

    # update messages if copy update is needed
    # thread_messages = [
    #    "Grooooovy! You're on the leaderboard for %s with a time of %f (speedy!!) and a character count of %d! Also, we made you an *awesome* video of your run!\r\nCheck your spot on the leaderboards here: %s\r\nRemix their solution: %s",
    #    "Woohoo!! You're on the leaderboard for %s with a time of %f (vroom vroom!) and a character count of %d! Check out this super cool video of your run!\r\nCheck your spot on the leaderboards here: %s\r\nRemix their solution: %s",
    #    "🥳🥳🥳 You're on the %s leaderboard with a super speedy time of %f and a character count of %d! We even made this groovy video of your run!\r\nCheck your spot on the leaderboards here: %s\r\nRemix their solution: %s",
    #    "Cowabunga! You've made it onto the %s leaderboard! You got an unbelievably fast time of %f (WOW!) and a character count of %d! There's even a super cool video of your run!\r\nCheck your spot on the leaderboards here: %s\r\nRemix their solution: %s",
    # ]

    submission_url = url_prefix + "?" + lztranscoder.compressToBase64(json.dumps(exploded_puzzle_data))

    # See if we have already scored this submission
    cached_result = persistence.get_submission_with_url(submission_url)
    if cached_result is not None:
        print("Invalid (duplicate) submission...")
        persistence.complete_queued_work(tweet_id)
        notify_user_highscore_already_exists(player_name, tweet_id, cached_result)
        return

    try:
        response = requests.post(url=scoring_service_uri, json={"level": submission_url}, verify=False)

        if response.status_code != 200:
            notify_user_unknown_error(player_name, tweet_id)
            persistence.complete_queued_work(tweet_id)

        score_data = json.loads(response.text)
        persistence.add_leaderboard_entry(player_name, score_data, submission_url)

        # Mark this job as complete
        persistence.complete_queued_work(tweet_id)

        if "time" not in score_data or score_data["time"] is None:
            print("Invalid (>30s) submission...")
            msg = "Sorry, that submission takes longer than 30 seconds to evaluate, so we had to disqualify it. :( Try again with a new solution!"
            twitter_client.post_tweet(msg, tweet_id)
        else:
            print("Successful submission!")
            msg = random.choice(responses) % (
                score_data["level"], score_data["time"], score_data["charCount"], leaderboard_uri)

            try:
                # Upload video...
                media_ids = twitter_client.upload_media(score_data["gameplay"], "video/mp4")

                # Respond to the submission thread
                print("Replying to submission thread")
                twitter_client.post_tweet(msg, tweet_id, media_ids)

                # Post on the original thread challenging others
                original_thread_id = persistence.get_config("twitter_%s" % puzzle_id, None)
                if original_thread_id is not None:
                    print("Replying to original thread (%s)" % (original_thread_id))
                    message = "We've just gotten a new submission in from {}! Can you beat them?".format(player_name)
                    twitter_client.post_tweet(message, original_thread_id, media_ids, use_primary_bot=True)
            except Exception as e:
                print(e)

    except Exception as e:
        metrics.incr("error.scoring_failure", 1)
        traceback.print_exc()

        # We need to eventually give up...
        if attempts >= 3:
            notify_user_unknown_error(player_name, tweet_id)
            persistence.complete_queued_work(tweet_id)

def process_work_queue():
    asyncio.run(process_work_queue_async())

async def process_work_queue_async():
    """ Attempt to process everything in the work queue. """
    try:
        print("Processing work queue")
        metrics.incr("workqueue.start", 1)
        queued_work = persistence.get_all_queued_work()
        tasks = []
        for work in queued_work:
            metrics.incr("workqueue.work", 1)
            tasks.append(do_scoring(work))
        print("PRE asyncio.gather")
        await asyncio.gather(*tasks)
        print("POST asyncio.gather")

    except Exception as e:
        metrics.incr("error.workqueue", 1)
        print("Exception: %s" % e)
    print("Work queue end")
    sys.stdout.flush()


def start_work_queue_polling():
    """ Attempts to process the work queue every 10 seconds. """
    polling.poll(process_work_queue, step=10, poll_forever=True)


def start_refresh_token_polling():
    """ Attempts to refresh all user tokens every 5 minutes. """
    polling.poll(twitter_client.refresh_all_tokens, step=60, poll_forever=True)


def start_submission_tweet_polling():
    """ Attempts to poll Twitter seconds for new submissions to process.  NOTE: this polls every 16
        seconds, which is very much by design (maximum # of requests for GET_2_tweets_search_recent
        is 60 per 15 minutes - see https://developer.twitter.com/en/docs/twitter-api/rate-limits)"""
    polling.poll(twitter_client.queue_new_tweet_submissions, step=16, poll_forever=True)


if AUTHORIZE_MANUALLY:
    twitter_client.force_user_authentication()


def post_test_tweets():
    # This tweet should timeout
    twitter_client.post_tweet("#testrider puzzle_1 x%s" % (str(random.randint(-1000000, 1000000))))

    # This is a valid submission that should be scored
    rand_num = random.randint(0, 1000000)
    solution = '.001x^2\\cdot .001x^4-2\\ +\\ -.05x^2\\ +\\ \\left(.5\\log \\left(x\\right)+5\\right)+\\sin \\left(17t\\right)' \
        .replace(" ", "")
    twitter_client.post_tweet("#testrider puzzle_21 %s+%d-%d" % (solution, rand_num, rand_num))

    # This tweet should result in an "invalid puzzle" response
    twitter_client.post_tweet("#testrider puzzle_234234 x%s" % (str(random.randint(-1000000, 1000000))))


if TESTING:
    post_test_tweets()


def start_server():
    """ Starts up the server. """
    app.run(port=8080, debug=True, use_reloader=False)


if "PROC_TYPE" not in os.environ:
    print("PROC_TYPE=null (probably running locally)")
    threading.Thread(target=start_server).start()
    threading.Thread(target=start_work_queue_polling).start()
    threading.Thread(target=start_refresh_token_polling).start()
    threading.Thread(target=start_submission_tweet_polling).start()
elif os.environ["PROC_TYPE"] == "web":
    print("PROC_TYPE=web, starting server...")
    threading.Thread(target=start_server).start()
elif os.environ["PROC_TYPE"] == "worker":
    print("PROC_TYPE=worker, starting polling...")
    threading.Thread(target=start_work_queue_polling).start()
    threading.Thread(target=start_refresh_token_polling).start()
    threading.Thread(target=start_submission_tweet_polling).start()
else:
    print("INVALID WORKER TYPE")
