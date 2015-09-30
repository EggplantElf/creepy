#!/usr/bin/python
# -*- coding: utf-8 -*-


import tweepy
from pymongo import MongoClient
import sys

# filter the tweets, users into "real" switch db, 
# based on the filtered german words 
# while keep the blacklist of false positive german words 

def write_words(words_file):
    f = open(words_file, 'w')
    client = MongoClient()
    for word in client['switch']['words'].find():
        f.write('%s\n' % word['word'])
    f.close()


def read_words(words_file):
    valid_wlist = set()
    for line in open(words_file):
        valid_wlist.add(line.strip())
    return valid_wlist


def filter_tweets(db, valid_wlist):
    client = MongoClient()
    tweets = client[db]['tweets']
    users = client[db]['users']
    strict_tweets = client[db]['strict_tweets']
    for tweet in tweets.find():
        valid_words = [w for w in tweet['words'] if w in valid_wlist]
        if valid_words:
            strict_tweets.insert({'tweet_id': tweet['tweet_id'],
                                  'user_id': tweet['user_id'],
                                  'text': tweet['text'],
                                  'words': valid_words})
            users.update({'user_id': tweet['user_id']},\
                         {'$inc': {'count': 1}}, upsert = True)




if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == '-w':
        write_words('de_words.txt')
    elif len(sys.argv) == 2 and sys.argv[1] == '-f':
        valid_wlist = read_words('de_words.txt')
        filter_tweets('switch', valid_wlist)
    else:
        print >> sys.stderr, 'wrong argument'

