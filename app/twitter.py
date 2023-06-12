import re
import sys
import traceback

import tweepy
import uuid
import requests
import os
from datetime import datetime, timedelta


class TwitterClient:
    def __init__(self, persistence, credentials_json, redirect_uri, testing):
        """ Constructor
        :param persistence: Persistence API
        :param credentials_json: Twitter credentials
        :param redirect_uri: URI to redirect to after authentication
        """
        self.persistence = persistence
        self.credentials_json = credentials_json
        self.v11_creds = credentials_json["v11_tokens"]
        self.v11_client_counter = 0
        self.v20_creds = credentials_json["v20_tokens"]
        self.v20_client_counter = 0
        self.redirect_uri = redirect_uri
        self.scopes = ["tweet.read", "users.read", "tweet.write", "offline.access"]
        self.testing = testing

    def post_tweet(self, text, in_reply_to_tweet_id=None, media_ids=None, use_primary_bot=False):
        """ Post a tweet
        :param text: The text of the tweet
        :param in_reply_to_tweet_id: (optional) The twitter tweet ID that you want to respond to
        :param media_ids: (optional) A list of Twitter media IDs that you want to upload alongside this tweet
        :param use_primary_bot: (optional) Whether or not to return the primary bot, or a random one from the pool
        """
        print("Posting tweet to %s with text: %s" % (in_reply_to_tweet_id, text))
        self.__get_next_v20_client(use_primary_bot). \
            create_tweet(text=text, user_auth=False, in_reply_to_tweet_id=in_reply_to_tweet_id, media_ids=media_ids)

    def upload_media(self, media_uri, file_type):
        """ Upload media to twitter
        :param media_uri: A uri that points to a piece of media
        :param file_type: MIME type of the media you wish to upload
        :return: an integer (media id) or None
        """
        filename = "./%s-gameplay.%s" % (uuid.uuid4(), file_type.split("/")[1])
        try:
            r = requests.get(media_uri, allow_redirects=True)
            file = open(filename, 'wb')
            file.write(r.content)
            file.flush()
            file.close()
            media = self.__get_next_v11_client().chunked_upload(filename, file_type=file_type,
                                                                additional_owners=self.get_all_owners())
            return [media.media_id_string]
        except Exception as e:
            print(e)
        finally:
            if os.path.exists(filename):
                os.remove(filename)
        return None

    def force_user_authentication(self):
        """ Forces all users managed by this module to login manually.  This only needs to be done once."""
        for config in self.v20_creds:
            self.__login(config)

    def refresh_all_tokens(self):
        """ Forces all users managed by this module to refresh their authentication tokens.  This needs to happen
        periodically, and should be done no less than 2 hours. """
        for config in self.v20_creds:
            self.__refresh(config)

    def __find_submissions_since(self, since_id=None):
        """ Returns all submissions posted since the last (newest) tweet we've processed.  Does the work here to
            make paginated requests to the Twitter API and returns back clean, valid Sinerider submissions with only
            relevant information we need to process jobs.
            NOTE: we only return submissions that are less than 1 day old and more recent than input context 'since_id'
        :param since_id: Only tweets posted after the given ID will be returned.
        :return: A list of objects containing submission information such as the puzzle ID, expression, and author info
        """
        print("Polling Twitter for submissions...")
        tweets = []

        expansions = ["author_id", "entities.mentions.username", "in_reply_to_user_id"]
        tweet_fields = ["author_id", "created_at"]
        user_fields = ["name", "username"]
        next_token = None
        newest_id = None
        yesterday = datetime.today() - timedelta(days=1)
        start_time = yesterday if since_id is None else None

        while True:
            response = self.__get_next_v20_client(False).search_recent_tweets("My solution for the #sinerider puzzle of the day",
                                                                              expansions=expansions,
                                                                              tweet_fields=tweet_fields,
                                                                              user_fields=user_fields,
                                                                              since_id=since_id,
                                                                              next_token=next_token,
                                                                              start_time=start_time,
                                                                              max_results=10)

            # Subsequent searches should just use the next_token
            since_id = None

            # Create a dictionary of the associated users
            users = {}
            if "users" in response.includes and len(response.includes["users"]) > 0:
                for user in response.includes["users"]:
                    users[user["id"]] = user

            response_data = response.data if response.data is not None else []
            for tweet in response_data:
                submission = self.__get_tweet_submission(tweet, users[tweet["author_id"]])
                if submission is not None:
                    tweets.append(submission)

            # if we got some results, we almost certainly have a new newest tweet id
            if "newest_id" in response.meta:
                newest_id = response.meta["newest_id"]

            if "next_token" in response.meta:
                next_token = response.meta["next_token"]
            else:
                break

        if newest_id is not None:
            self.persistence.set_config("newest_twitter_id", newest_id)

        print("Done polling Twitter!")

        return tweets

    def __get_tweet_submission(self, tweet, user_info):
        text = tweet["text"]
        author_id = tweet["author_id"]
        if not self.testing and author_id in self.get_all_owners():
            print("Detected bot tweet, ignoring...")
            return "We don't respond to bots"

        match = re.search(
            r"#(?P<puzzle_id>puzzle_[0-9]+)(?P<middle>.*characters)(?P<expression>.*)(Try solving it yourself: .+)",
            text, re.MULTILINE | re.DOTALL)
        if match:
            puzzle_id = match.group("puzzle_id")
            expression = match.group("expression").strip()

            return {
                "author_id": tweet["author_id"],
                "author_username": user_info["name"],
                "id": tweet["id"],
                "text": text,
                "puzzle_id": puzzle_id,
                "expression": expression
            }
        else:
            print("Not a valid submission tweet, ignoring.  text: %s" % (text))
            return None

    def queue_new_tweet_submissions(self):
        """ Performs a twitter search for all new tweet submissions.  Queues all valid new submissions found on twitter
            into the work queue for later processing by the worker responsible for draining."""

        newest_tweet_id = self.persistence.get_config("newest_twitter_id", None)

        try:
            submissions = self.__find_submissions_since(newest_tweet_id)
            print("New submissions: %d" % (len(submissions)))
            for submission in submissions:
                try:
                    self.persistence.queue_work(str(submission["id"]), submission["author_username"],
                                                submission["puzzle_id"], submission["expression"])
                except Exception as e:
                    traceback.print_exc()
        except Exception as e:
            traceback.print_exc()

    def get_all_owners(self):
        """ Gets an array of all twitter user ids associated with the v2.0 credential pool
        :return: An array of twitter user ids
        """
        return map(lambda config: config["twitter_user_id"], self.v20_creds)

    def __get_next_v20_client(self, use_primary_bot):
        """ Returns a valid tweepy v2.0 twitter client

        :param use_primary_bot: Instead of returning the next client from the pool, return the main twitter bot.
            This is particularly useful for supporting publishing puzzles via one singular account.
        :return: A tweepy Client (v2.0)
        """
        self.v20_client_counter += 1
        index = 0 if use_primary_bot else self.v20_client_counter % len(self.v20_creds)
        config = self.v20_creds[index]
        bearer_token = self.persistence.get_config("user_bearer_token_%s" % (config["client_id"]), "<unknown>")
        return tweepy.Client(bearer_token)

    def __get_next_v11_client(self):
        """ Returns a valid tweepy v1.1 twitter client
        :return: A tweepy API (v1.1)
        """
        self.v11_client_counter += 1
        config = self.v11_creds[self.v11_client_counter % len(self.v11_creds)]

        return tweepy.API(tweepy.OAuth1UserHandler(config["consumer_key"],
                                                   config["consumer_secret"],
                                                   config["access_token"],
                                                   config["access_token_secret"]))

    def __login(self, credentials):
        """ Forces the user to manually login given a set of credentials
        :param credentials: v2.0 authentication configuration
        """

        bearer_token_key = "user_bearer_token_%s" % (credentials["client_id"])
        refresh_token_key = "user_refresh_token_%s" % (credentials["client_id"])

        oauth2_user_handler = tweepy.OAuth2UserHandler(
            client_id=credentials["client_id"],
            redirect_uri=self.redirect_uri,
            scope=self.scopes,
            client_secret=credentials["client_secret"]
        )
        authorization_response_url = input("Please navigate to the url: " + oauth2_user_handler.get_authorization_url())
        access_token = oauth2_user_handler.fetch_token(
            authorization_response_url
        )

        self.persistence.set_config(bearer_token_key, access_token["access_token"])
        self.persistence.set_config(refresh_token_key, access_token["refresh_token"])

    def __refresh(self, credentials):
        """ Forces a set of credentials to be refreshed using the PKCE Refresh Token Flow
            See https://developer.twitter.com/en/docs/authentication/oauth-2-0/authorization-code for more details
        :param credentials: v2.0 authentication configuration
        """

        bearer_token_key = "user_bearer_token_%s" % (credentials["client_id"])
        refresh_token_key = "user_refresh_token_%s" % (credentials["client_id"])

        # Note - this is a required workaround, otherwise refresh token doesn't work
        class MyOAuth2UserHandler(tweepy.OAuth2UserHandler):
            def refresh_token(self, refresh_token):
                new_token = super().refresh_token(
                    "https://api.twitter.com/2/oauth2/token",
                    refresh_token=refresh_token,
                    body=f"grant_type=refresh_token&client_id={self.client_id}",
                )
                return new_token

        refresh_token = self.persistence.get_config(refresh_token_key, "<null>")
        auth = MyOAuth2UserHandler(
            client_id=credentials["client_id"],
            redirect_uri=self.redirect_uri,
            scope=self.scopes,
            client_secret=credentials["client_secret"]
        )
        access_token = auth.refresh_token(refresh_token)
        self.persistence.set_config(bearer_token_key, access_token["access_token"])
        self.persistence.set_config(refresh_token_key, access_token["refresh_token"])
        print("refreshed auth token for client id %s" % (credentials["client_id"]))
