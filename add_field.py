#!/usr/bin/python
# -*- coding: utf-8 -*-

import tweepy
import json
import sys
from pymongo import MongoClient



def add_field(client, lang):
    tweets = client['twitter_'+lang]['tweets']
    tweets.update({}, {'$set': {'indexed': False}}, upsert = True, multi = True)


if __name__ == '__main__':
    client = MongoClient()
    for lang in ['en', 'de', 'tr']:
        add_field(client, lang)