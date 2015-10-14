#!/usr/bin/python
# -*- coding: utf-8 -*-


import tweepy
import json
import sys


def find(uid):
    data = open('pwd/search.pwd').read()
    infos = json.loads(data)
    consumer_key = infos['consumer_key']
    consumer_secret = infos['consumer_secret']
    auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    user = api.get_user(uid)
    print 'https://twitter.com/' + user.screen_name


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'use the user_id as argument, e.g. 2259129079'
        exit(0)
    else:
        uid = sys.argv[1]
        find(uid)