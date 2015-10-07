#!/usr/bin/python
# -*- coding: utf-8 -*-

import tweepy
import json
import sys
from pymongo import MongoClient
from pprint import pprint


# TODO
# write tweets from the users into DB (add in switch.tweets)
# clear redundant tweets (caution)


class Searcher:
    def __init__(self, source_db, target_db, auth_file):
        self.authorize(auth_file)
        self.client = MongoClient()
        self.users = self.client[source_db]['users']
        self.tweets = self.client[target_db]['tweets']
        self.error = 0

    def authorize(self, auth_file):
        data = open(auth_file).read()
        infos = json.loads(data)
        consumer_key = infos['consumer_key']
        consumer_secret = infos['consumer_secret']

        auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
        self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


    def search_all(self):
        try:
            for user in self.users.find({'searched': False}):
                success = self.search(user['user_id'])
                self.users.update({'_id': user['_id']}, {'$set': {'searched': True}})                    
                if not success:
                    self.users.update({'user_id': user['user_id']}, {'$set': {'flag': True}})
        except:
            self.error += 1
            if self.error < 10:
                self.search_all()


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
    if len(sys.argv) == 3:
        source_db = sys.argv[1]
        target_db = sys.argv[2]
        searcher = Searcher(source_db, target_db, 'pwd/search.pwd')
        searcher.search_all()
    else:
        print 'Wrong argument'