#!/usr/bin/python
# -*- coding: utf-8 -*-

import tweepy
import json
import sys

def check_exist(input_file, output_file):
    data = open('pwd/search.pwd').read()
    infos = json.loads(data)
    consumer_key = infos['consumer_key']
    consumer_secret = infos['consumer_secret']
    auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    f = open(output_file, 'w')

    for line in open(input_file):
        items = line.split(',', 3)
        tid = items[1]
        try:
            t = api.get_status(tid)
            f.write(line)
            print 'yeah'
        except:
            print 'nope'
    f.close()

if __name__ == '__main__':
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    check_exist(input_file, output_file)