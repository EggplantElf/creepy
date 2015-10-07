#!/usr/bin/python
# -*- coding: utf-8 -*-

import tweepy
import json
import sys
from pymongo import MongoClient



def add_field(client, db, collection, key, value):
    # tweets = client['twitter_'+lang]['tweets']
    tweets = client[db][collection]
    tweets.update({}, {'$set': {key: value}}, upsert = True, multi = True)


if __name__ == '__main__':
    client = MongoClient()
    db = sys.argv[1]
    collection = sys.argv[2]
    key = sys.argv[3]
    value_str = sys.argv[4]
    value = True if value_str in ['true', 'True'] else False
    add_field(client, db, collection, key, value)