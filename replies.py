import tweepy
import time
import os
from dotenv import load_dotenv

load_dotenv()

auth = tweepy.OAuthHandler(os.environ["API_KEY"], os.environ["API_KEY_SECRET"])
auth.set_access_token(os.environ["ACCESS_TOKEN"], os.environ["ACCESS_TOKEN_SECRET"])
api = tweepy.API(auth)

bot_id = int(api.verify_credentials().id_str)
mention_id = 1
message = "Wahoo! I've submitted your answer!"


#while True:
#    mentions = api.mentions_timeline(since_id=mention_id)
#    for mention in mentions:
#        print("Mention found! Replying to: " + mention.user.screen_name + "mention saying: " + mention.text)
#        mention_id = mention.id
#        if mention.in_reply_to_status_id is None and mention.user.id != bot_id:
#            if True in [word in mention.text.lower() for word in ["answer"]]:
#                try:
#                    print("Replying to: " + mention.user.screen_name + "mention saying:" + mention.text)
#                    api.update_status(status=message, in_reply_to_status_id=mention.id)
#                    print("Success!! Replied to: " + mention.user.screen_name + "mention saying:" + mention.text)
#                except Expection as exc:
#                    print(exc)
#    time.sleep(30)