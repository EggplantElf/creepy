#!/usr/bin/python
# -*- coding: utf-8 -*-


import tweepy
import re
import json
from pymongo import MongoClient
from segtok.segmenter import split_multi
from segtok.tokenizer import word_tokenizer, split_contractions


# filter @username, #topic and url
filter_pattern = r'(@|#|https?:)\S*'


def load_dict(de_file, tr_file):
    de_words, tr_words = set(), set()
    for line in open(de_file):
        de_words.add(line.strip())
    for line in open(tr_file):
        tr_words.add(line.strip())
    return de_words, tr_words



def check(de_words, tr_words):
    client = MongoClient()
    tweets = client['twitter_tr']['tweets']

    i = 0
    for tweet in tweets.find({'indexed': False}): #.limit(batch_size)
        # tweets.update({'_id': tweet['_id']}, {'$set': {'indexed': True}})
        i += 1
        text = re.sub(filter_pattern, '', tweet['text'])
        total, de, tr = 0, 0, 0
        de_list, tr_list = [], []
        ans = False
        for sent in split_multi(text):
            for word in word_tokenizer(sent):
                total += 1
                if word in tr_words:
                    tr += 1
                    tr_list.append(word)
                elif word in de_words: 
                    de += 1
                    de_list.append(word)
                if tr >= 5 and de >= 2:
                    ans = True
        if ans:
            print tweet['text'].encode('utf-8')
            print de_list

        if i % 10000 == 0:
            print '*****************'
            print 'read %d tweets'
            print '*****************'



if __name__ == '__main__':
    de_words, tr_words = load_dict('dict_de.txt', 'dict_tr.txt')
    check(de_words, tr_words)
