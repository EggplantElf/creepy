#!/usr/bin/python
# -*- coding: utf-8 -*-


import tweepy
import re
import json
from pymongo import MongoClient
from segtok.segmenter import split_multi
from segtok.tokenizer import word_tokenizer, split_contractions

# TODO:
# loose check tweets for the first time
# store potential code_switch user into db
# check all tweets of these users
# since there will be other source of tweets, check tweet_id for duplication
# mentions


# filter @username, #topic and url
filter_pattern = r'(@|#|https?:)\S*'


class Checker:
    def __init__(self, de_file, tr_file):
        self.de_words = set()
        self.tr_words = set()
        self.load_dict(de_file, tr_file)
        self.client = MongoClient()
        self.switch_tweets = self.client['switch']['tweets']
        self.switch_users = self.client['switch']['users']
        self.switch_words = self.client['switch']['words']
        self.switch_mentions = self.client['switch']['mentions']


    def load_dict(self, de_file, tr_file):
        for line in open(de_file):
            self.de_words.add(line.strip())
        for line in open(tr_file):
            self.tr_words.add(line.strip())



    def check(self, db):
        tweets = self.client[db]['tweets']
        i = 0
        for tweet in tweets.find({'indexed': False}): #.limit(batch_size)
            # tweets.update({'_id': tweet['_id']}, {'$set': {'indexed': True}})
            i += 1
            text = re.sub(filter_pattern, '', tweet['text'])
            total, de, tr = 0, 0, 0
            de_list, tr_list = [], []
            for sent in split_multi(text):
                for word in word_tokenizer(sent):
                    word = word.lower() # CAUTION!!!
                    total += 1
                    if word in self.tr_words:
                        tr += 1
                        tr_list.append(word)
                    elif word in self.de_words: 
                        de += 1
                        de_list.append(word)

            # TODO: make the constraints more configurable
            if tr >= 4 and de >= 1:
                # only log if the tweet is not already in switch_tweets
                if not self.switch_tweets.find({'tweet_id': tweet['tweet_id']}):
                    self.log(tweet, de_list)

            if i % 10000 == 0:
                print '*****************'
                print 'read %d tweets' % i
                print '*****************'


    def log(self, tweet, de_list):
        print tweet['text'].encode('utf-8')
        print de_list
        ################
        # log the tweet
        self.switch_tweets.insert({'tweet_id': tweet['tweet_id'],\
                                   'user_id' :tweet['user_id'],\
                                   'text': tweet['text'],\
                                   'words': de_list})
        # log the user
        self.switch_users.update({'user_id': tweet['user_id']},\
                                 {'$inc': {'count': 1}}, upsert = True)

        # log the german words
        for word in de_list:
            self.switch_words.update({'word': word},\
                                     {'$inc': {'count': 1}}, upsert = True)

        # log the mentinos
        # self.switch_mentions.insert()


if __name__ == '__main__':
    checker = Checker('dict_de.txt', 'dict_tr.txt')
    checker.check('twitter_tr')
