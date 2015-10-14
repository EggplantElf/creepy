#!/usr/bin/python
# -*- coding: utf-8 -*-


import tweepy
import time
import json
import re
import sys
import readline
from pymongo import MongoClient


class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'


class Annotator:
    def __init__(self, db):
        self.tweets = MongoClient()[db]['tweets']

    def start(self):
        for tweet in self.tweets.find({'flag': 'x'}):
            oid = tweet['_id']
            text = tweet['text']
            words = tweet['words']
            self.show(text, words)
            print color.CYAN + ', '.join(w.encode('utf-8') for w in words) + color.END

            flag = 'x'
            input_str = raw_input()
            # simple mark
            if input_str in ['s', 'c', 'g', 't', 'n', 'p', 'x']:
                flag = input_str
                self.mark(oid, flag)
            # mark all tweets with the german word(s) in the list
            elif len(input_str) == 2 and input_str[0] == 'a':
                flag = input_str[1]
                self.mark_all(tweet['words'], flag)

    # show highlighted text
    def show(self, text, words):
        text = text.encode('utf-8')
        for word in words:
            text = re.sub(r'(%s)'%word, color.RED + r'\1' + color.END, text)
        print text


    def mark(self, oid, flag):
        self.tweets.update({'_id': oid}, {'$set': {'flag': flag}})


    def mark_all(self, words, flag):
        self.tweets.update({'$and': [{'words':{'$size': 1}}, {'words':{'$in': words}}]},\
                            {'$set': {'flag': flag}}, multi = True)


if __name__ == '__main__':
    db = sys.argv[1]
    annotator = Annotator(db)
    annotator.start()
    # annotator.show(u'Über daß çok, ok', [u'daß', u'ok'])