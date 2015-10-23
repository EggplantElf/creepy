#!/usr/bin/python
# -*- coding: utf-8 -*-

import tweepy
import json
import sys
from pymongo import MongoClient



def check_exist(input_file, output_file):
    data = open('pwd/search.pwd').read()
    infos = json.loads(data)
    consumer_key = infos['consumer_key']
    consumer_secret = infos['consumer_secret']
    auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    f = open(output_file, 'w')

    tweets = {}

    for line in open(input_file):
        items = line.split(',', 3)
        tid = items[1]
        tweets[int(tid)] = line

    keys = sorted(tweets.keys())
    i = 0
    total = 0
    count = 0
    while i * 100 < len(tweets):
        tids = keys[i * 100 : (i+1) * 100]
        total += len(tids)
        i += 1
        results = api.statuses_lookup(tids, trim_user=True)
        count += len(results)
        for t in results:
            f.write(tweets[t.id])
    f.close()
    print 'checked %d tweets, %d still exist' % (total, count)


def check_exist_in_db(source_db, target_db):
    data = open('pwd/search.pwd').read()
    infos = json.loads(data)
    consumer_key = infos['consumer_key']
    consumer_secret = infos['consumer_secret']
    auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


    tweets_dict = {}
    tweets_list = []

    client = MongoClient()
    source = client[source_db]['tweets']
    target = client[target_db]['tweets']
    for tweet in source.find():
        tid = tweet['tweet_id']
        tweets_list.append(tid)
        tweets_dict[tid] = tweet


    i = 0
    total = 0
    count = 0
    while i * 100 < len(tweets_list):
        tids = tweets_list[i * 100 : (i+1) * 100]
        total += len(tids)
        i += 1
        results = api.statuses_lookup(tids, trim_user=True)
        count += len(results)
        for t in results:
            print tweets_dict[str(t.id)]
            target.insert(tweets_dict[str(t.id)])
            
    print 'checked %d tweets, %d still exist' % (total, count)



if __name__ == '__main__':
    # input_file = sys.argv[1]
    # output_file = sys.argv[2]
    # check_exist(input_file, output_file)
    source_db = sys.argv[1]
    target_db = sys.argv[2]
    check_exist_in_db(source_db, target_db)