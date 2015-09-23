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



def index(client, freq_file, lang, batch_size):
    freq = client['freq'][lang]
    tweets = client['twitter_'+lang]['tweets']

    freq_dict = load(freq_file)

    # read a batch, 
    # update 'indexed', 
    # increase count in freq_dict and total
    t0 = time()

    for tweet in tweets.find({'indexed': False}).limit(batch_size):
        # tweets.update({'_id': tweet['_id']}, {'$set': {'indexed': True}})
        text = re.sub(filter_pattern, '', tweet['text'])
        for sent in split_multi(text):
            for word in word_tokenizer(sent):
                freq_dict[word] += 1


    t1 = time() - t0
    print 'time used to read tweets:', t1
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
    size = int(sys.argv[2])
    freq_file = 'freq_%s.txt' % lang

    client = MongoClient()
    index(client, freq_file, lang, size)
