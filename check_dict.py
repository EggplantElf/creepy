#!/usr/bin/python
# -*- coding: utf-8 -*-


import tweepy
import time
import json
import re
import sys
from pymongo import MongoClient
from segtok.segmenter import split_multi
from segtok.tokenizer import word_tokenizer, split_contractions
from collections import defaultdict

# NOTE:
# Title() for morph analysis, lower() for dictionary lookup
# ALWAYS: decode().lower().encode()
# encapsule the lower() and title()


# filter @username, #topic and url
filter_pattern = re.compile(r'(@|#|https?:)\S*')


def read_dict(freq_file):
    d = defaultdict(int)
    for line in open(freq_file):
        items = line.strip().split()
        w, f = items[0], int(items[1])
        d[w] = f
    return d

class Checker:
    """
    for the speed of morphological analyzer, it has to work in batch,
    it analyzes e.g. every 1000 tweets in one go, then reloads the analyzer
    """

    def __init__(self, source_db, target_db, de_dict_file, tr_dict_file):
        self.client = MongoClient()
        self.source_db = self.client[source_db]
        self.target_db = self.client[target_db]
        self.de_dict = read_dict(de_dict_file)
        self.tr_dict = read_dict(tr_dict_file)


    # generator for reading tweets from db
    def tweet_stream(self):
        for tweet in self.source_db['tweets'].find():
            # change the state of the tweet as indexed (checked)
            # self.source_db['tweets'].update({'_id': tweet['_id']}, {'$set': {'indexed': True}}, upsert = True)
            yield (tweet['text'], tweet['tweet_id'], tweet['user_id']) 


    def tokenize(self, text):
        """
        tokenize the text and filter the @username (and punctuation, smiley ...), leave only words
        """
        words = [] # list of words
        # text = text.decode('utf-8')
        text = filter_pattern.sub(' ', text)
        for sent in split_multi(text):
            for token in word_tokenizer(sent):
                words.append(token.encode('utf-8', 'ignore'))
        return words


    def check(self):
        for (text, tid, uid) in self.tweet_stream():
            words = self.tokenize(text)
            codes = [self.code(word) for word in words]
            print text
            print words
            print codes

            # if de_list:
                # self.log(text, tid, uid, de_list)

    def code(self, word):
        if word in self.tr_dict:
            return 'tr'
        elif word in self.de_dict:
            return 'de'
        else:
            return 'xx'



    def log(self, text, tid, uid, de_list):
        print text.encode('utf-8')
        print '[' + ', '.join(de_list) + ']'
        ################
        # log the tweet
        self.target_db['tweets'].insert({'tweet_id': tid,\
                                   'user_id': uid,\
                                   'text': text,\
                                   'words': de_list})
        # log the user
        self.target_db['users'].update({'user_id': uid},\
                                 {'$inc': {'count': 1}}, upsert = True)

        # log the german words
        for word in de_list:
            self.target_db['words'].update({'word': word},\
                                     {'$inc': {'count': 1}}, upsert = True)



if __name__ == '__main__':
    source_db = sys.argv[1]
    target_db = sys.argv[2]
    checker = Checker(source_db, target_db, 'freq_de.no_en.txt', 'freq_tr.no_en.txt')
    checker.check()
