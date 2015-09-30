#!/usr/bin/python
# -*- coding: utf-8 -*-

import tweepy
import json
from pymongo import MongoClient
from pprint import pprint


# TODO
# write tweets from the users into DB (add in switch.tweets)
# clear redundant tweets (caution)


class Searcher:
    def __init__(self, auth_file):
        self.authorize(auth_file)
        self.client = MongoClient()
        self.users = self.client['switch']['users']
        self.tweets = self.client['core_user']['tweets']


    def authorize(self, auth_file):
        data = open(auth_file).read()
        infos = json.loads(data)
        consumer_key = infos['consumer_key']
        consumer_secret = infos['consumer_secret']

        auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
        self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


    def search_all(self):
        for user in self.users.find({'searched': False}):
            success = self.search(user['user_id'])
            self.users.update({'_id': user['_id']}, {'$set': {'searched': True}})                    
            if not success:
                self.users.update({'user_id': user['user_id']}, {'$set': {'flag': True}})

    def search(self, uid):
        try:
            for tweet in tweepy.Cursor(self.api.user_timeline, user_id=uid, count=200).items(3200):
                self.tweets.insert({'text': tweet.text,\
                                    'tweet_id': tweet.id_str,\
                                    'user_id': tweet.author.id_str,\
                                    'indexed': False})
            return True
        except:
            return False

    def rate_left(self):
        status = self.api.rate_limit_status()
        print status['resources']['statuses']['/statuses/user_timeline']


if __name__ == '__main__':
    searcher = Searcher('pwd/search.pwd')
    searcher.search_all()
    # searcher.rate_left()