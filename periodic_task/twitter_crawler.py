import re
import pandas as pd

import twitter
from base.config import crawler_logger as logger
from db.base_model import sc_wrapper
from db.model import Factor

USERS = ['@twitter',
         '@twitterapi',
         '@support']

LANGUAGES = ['en']


class TwitterConfig(object):
    CONSUMER_KEY = "PIL6NGF3IIJ98qtR5Gl2LEsKh"
    CONSUMER_SECRET = "6xKfr07f6e8vYSegmwY5d4zgavSdwjr96mwpbKgOtqWgAjMizV"
    ACCESS_TOKEN_KEY = "2484774386-fteHp4cZRr296C3tNe2O4Zm4Z3OzDwlKFtqjkOI"
    ACCESS_TOKEN_SECRET = "kSwskugbAFu328AgHezfaJRenaFJ1LqN4E2fo0triAFuS"


def get_twitter_api():
    return twitter.Api(consumer_key=TwitterConfig.CONSUMER_KEY,
                       consumer_secret=TwitterConfig.CONSUMER_SECRET,
                       access_token_key=TwitterConfig.ACCESS_TOKEN_KEY,
                       access_token_secret=TwitterConfig.ACCESS_TOKEN_SECRET)


def get_tweets(api=None, screen_name=None):
    timeline = api.GetUserTimeline(screen_name=screen_name, count=200)
    earliest_tweet = min(timeline, key=lambda x: x.id).id
    print("getting tweets before:", earliest_tweet)

    while True:
        tweets = api.GetUserTimeline(
            screen_name=screen_name, max_id=earliest_tweet, count=200
        )
        new_earliest = min(tweets, key=lambda x: x.id).id

        if not tweets or new_earliest == earliest_tweet:
            break
        else:
            earliest_tweet = new_earliest
            print("getting tweets before:", earliest_tweet)
            timeline += tweets

    return timeline


@sc_wrapper
def record_tweets(timeline, sc=None):
    usd_pattern = re.compile(r'[(](.*?) USD[)]', re.S)
    coin_amount_pattern = re.compile(r'(.*?)[#]', re.S)
    coin_pattern = re.compile(r'[#](.*?)[(]', re.S)
    for tweet in timeline:
        text = tweet._json['text']
        if "from" in text and "to" in text:
            tweet_time = pd.to_datetime(tweet.created_at).to_pydatetime()
            favorite_count = tweet.favorite_count
            retweet_count = tweet.retweet_count
            light_number = text.count('🚨')
            text = text.replace('🚨', '')
            usd_number = float(usd_pattern.findall(tweet._json['text'])[0].replace(",", ""))
            from_addr = text.split(" from ")[-1].split(" to ")[0]
            to_addr = text.split(" from ")[-1].split(" to ")[-1].split("\n")[0]
            transfer_coin_name = coin_pattern.findall(tweet._json['text'])[0]
            coin_amount_str = re.sub("[^0-9]", "", coin_amount_pattern.findall(text)[0])
            coin_amount = float(coin_amount_str.replace(" ", "").replace(",", ""))
            data = {
                "usd_number": usd_number,
                "transfer_coin_name": transfer_coin_name,
                "coin_amount": coin_amount,
                "from_addr": from_addr,
                "to_addr": to_addr,
                "favorite_count": favorite_count,
                "retweet_count": retweet_count,
                "light_number": light_number
            }
            unique = f"{tweet_time}-{usd_number}-{coin_amount}-{from_addr}"
            object_ = sc.query(Factor).filter(Factor.unique_key == unique).first()
            if object_:
                object_.timestamp = tweet_time
                object_.source = "twitter_whale"
                object_.tag = transfer_coin_name
                object_.type = "transaction"
                object_.unique_key = unique
                object_.data = data
            else:
                factor = Factor(
                    timestamp=tweet_time,
                    source="twitter_whale",
                    tag=transfer_coin_name,
                    type="transaction",
                    unique_key=unique,
                    data=data
                )
                sc.add(factor)
            logger.info(f"{unique}")
