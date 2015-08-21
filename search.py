#!/usr/bin/python
# -*- coding: utf-8 -*-

import tweepy
import json
from pymongo import MongoClient

# NOTES
# query - RT (süper!)
# 100 per search
# 200 per timeline
# more pages 
# not too many keywords per query
# https://dev.twitter.com/oauth/application-only
# include_entities = False: url, hashtag

def read_auth_file(filename):
    data = open(filename).read()
    infos = json.loads(data)
    consumer_key = infos['consumer_key']
    consumer_secret = infos['consumer_secret']
    access_token = infos['access_token']
    access_token_secret = infos['access_token_secret']
    return consumer_key, consumer_secret, access_token, access_token_secret

def read_keywords(filename):
    keywords = []
    for line in open(filename):
        word = line.strip()
        if word:
            keywords.append(word)
    return keywords


def search_users(users, keywords):
    query = ' OR '.join('"%s"' % w for w in keywords) + '-RT'
    print query
    i = 1
    for tweet in tweepy.Cursor(api.search, q = query, lang = 'tr', count = 100).items(200):
        uid = str(tweet.user.id)
        print i, tweet.text
        i += 1
        users.update({'uid': uid}, {'$inc': {'count': 1}}, upsert = True) # update_one for hihger version





if __name__ == '__main__':
    consumer_key, consumer_secret, access_token, access_token_secret = read_auth_file('pwd/search.pwd')
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True)

    client = MongoClient()
    users = client['cs']['users']

    keywords = read_keywords('keywords.txt')
    search_users(users, keywords)

    # for tweet in api.user_timeline(user_id='865021117', count=200): #maximum count = 200
    #     print tweet.text
    #     print '------'