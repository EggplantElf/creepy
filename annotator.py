#!/usr/bin/python
# -*- coding: utf-8 -*-


import tweepy
import time
import json
import re
import sys
import readline
from pymongo import MongoClient



class Annotator:
    def __init__(self, db):
        self.tweets = MongoClient()[db]['tweets']
        self.size = 10

    def start(self):
        for tweet in self.tweets.find({'flag': 'x'}).limit(self.size):
            oid = tweet['_id']
            print tweet['text'].encode('utf-8')
            print tweet['words'].encode('utf-8')
            flag = 'x'
            input_str = raw_input()
            # simple mark
            if input_str in ['s', 'g', 't', 'n', 'p', 'x']:
                flag = input_str
                self.mark(oid, flag)
            # mark all tweets with the german word(s) in the list
            # elif len(input_str) == 2 and input_str[1] == 'w':
            #     flag = input_str[1]
            #     self.mark_all(tweet['words'], flag)

    def mark(oid, flag):
        self.tweets.update({'_id', oid}, {'$set': {'flag': flag}})





if __name__ == '__main__':
    db = sys.argv[1]
    annotator = Annotator(db)
    annotator.start()