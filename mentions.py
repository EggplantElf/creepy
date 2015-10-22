#!/usr/bin/python
# -*- coding: utf-8 -*-


import tweepy
from pymongo import MongoClient
import sys
import re

pattern = re.compile(r'@(\S)')



def process(db):
    client = MongoClient()
    tweets = client[db]['tweets']
    for t in tweets.find():
        m = pattern.search(t['text'])
        if m:
            print t['text']
            print m.group(1)



if __name__ == '__main__':
    db = sys.argv[1]

    process(db)