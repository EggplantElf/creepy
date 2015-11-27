#!/usr/bin/python
# -*- coding: utf-8 -*-

import tweepy
import json
import re
import sys
from pymongo import MongoClient
from collections import defaultdict
from segtok.segmenter import split_multi
from segtok.tokenizer import word_tokenizer, split_contractions
from time import time




# filter @username, #topic and url
filter_pattern = r'(@|#|https?:)\S*'



def index(client, freq_file, lang):
    tweets = client['twitter_'+lang]['tweets']
    # freq_dict = load(freq_file)
    freq_dict = defaultdict(int)

    i = 0
    for tweet in tweets.find():
        i += 1
        if i % 100000 == 0:
            print i
        # tweets.update({'_id': tweet['_id']}, {'$set': {'indexed': True}})
        # text = tweet['text'].lower()
        text = tweet['text']
        text = re.sub(filter_pattern, '', text)
        for sent in split_multi(text):
            for word in word_tokenizer(sent):
                freq_dict[word] += 1

    # for the second db (tr)
    tweets = client['new_'+lang]['tweets']
    for tweet in tweets.find():
        i += 1
        if i % 100000 == 0:
            print i
        # tweets.update({'_id': tweet['_id']}, {'$set': {'indexed': True}})
        # text = tweet['text'].lower()
        text = tweet['text']
        text = re.sub(filter_pattern, '', text)
        for sent in split_multi(text):
            for word in word_tokenizer(sent):
                freq_dict[word] += 1


    save(freq_file, freq_dict)

def read(origin_file, freq_file, lang):
    freq_dict = defaultdict(int)
    i = 0
    for line in open(origin_file):
        i += 1
        if i % 100000 == 0:
            print i
        items = line.strip().split(',', 3)
        if len(items) == 4 and items[0] == lang:
            # text = items[3].lower().decode('utf-8')
            text = items[3].decode('utf-8')
            text = re.sub(filter_pattern, '', text)
            for sent in split_multi(text):
                for word in word_tokenizer(sent):
                    freq_dict[word] += 1
    save(freq_file, freq_dict)




def load(freq_file):
    d = defaultdict(int)
    try:
        for line in open(freq_file):
            tmp = line.decode('utf-8').strip().split()
            d[tmp[0]] = int(tmp[1])
    except:
        pass
    return d


def save(freq_file, freq_dict):
    f = open(freq_file, 'w')
    for k, v in sorted(freq_dict.iteritems(), key = lambda (k, v) : v, reverse= True):
        line = '%s\t%d\n' % (k, v)
        f.write(line.encode('utf-8'))
    f.close()


if __name__ == '__main__':
    lang = sys.argv[1]
    freq_file = sys.argv[2]
    client = MongoClient()
    index(client, freq_file, lang)
    # origin_file = sys.argv[1]
    # freq_file = sys.argv[2]
    # lang = 'de'
    # read(origin_file, freq_file, lang)

