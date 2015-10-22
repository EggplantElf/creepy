#!/usr/bin/python
# -*- coding: utf-8 -*-


import tweepy
from pymongo import MongoClient
import sys
import json
import re

pattern = re.compile(r'@(\S+)')

def auth_api():
    data = open('pwd/search.pwd').read()
    infos = json.loads(data)
    consumer_key = infos['consumer_key']
    consumer_secret = infos['consumer_secret']
    auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api

def process(api, source_db, target_db):
    client = MongoClient()
    tweets = client[source_db]['tweets']
    users = client[source_db]['users']
    target_tweets = client[target_db]['tweets']
    mentioned_uid = set()
    mentioned_names = set()

    for t in tweets.find():
        # print t['text'].encode('utf-8')
        m = pattern.search(t['text'].encode('utf-8'))
        if m:
            name = m.group(1)
           # print name
            if name not in mentioned_names:
                mentioned_names.add(name)
                try:
                    uid = str(api.get_user(name).id)
                    if not (uid in mentioned_uid or users.find({'user_id': uid})):
                        print uid, name
                        mentioned_uid.add(uid)
                        search(uid)
                except:
                    pass


def search(uid):
    try:
        for tweet in tweepy.Cursor(self.api.user_timeline, user_id=uid, count=200).items(3200):
            if tweet.lang == 'tr':
                self.tweets.insert({'text': tweet.text,\
                                    'tweet_id': tweet.id_str,\
                                    'user_id': tweet.author.id_str,\
                                    'indexed': False})
        return True
    except:
        return False


if __name__ == '__main__':
    api = auth_api()
    source_db = sys.argv[1]
    target_db = sys.argv[2]
    process(api, source_db, target_db)
