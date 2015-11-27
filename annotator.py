#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import os
import sys
import time
import signal
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

HELPER = [('s', 'Code-Switch'),\
          ('c', 'Crap'),\
          ('g', 'Totally German'),\
          ('t', 'Totally Turkish'),\
          ('n', ''),\
          ('p', ''),\
          ('x', 'Undecided'),\
          ]

ALPHABET = u'[a-zA-ZßäöüÄÖÜçÇşŞğĞıİ]'

class Annotator:
    def __init__(self, db, user):
        self.client = MongoClient('mongodb://%s:%s@ds049538.mongolab.com:49538/%s' % (user, user, db))
        self.tweets = self.client[db]['tweets']
        self.user = user
        self.history = {}
        self.count = 0
        self.start_time = time.time()
        signal.signal(signal.SIGINT, self.goodbye)



    def goodbye(self, signal, frame):
        print color.PURPLE
        print 'You have annotated %d tweets in %d minutes' % (self.count, (time.time() - self.start_time) / 60)
        for k in self.history:
            print '%s:\t%s'%(k, self.history[k])
        print color.END
        exit(0)

    def start(self):
        for tweet in self.tweets.find({'annotations.%s' %self.user: {'$exists': False}}):
            # print chr(27) + "[2J"
            os.system('cls' if os.name == 'nt' else 'clear')
            oid = tweet['_id']
            text = tweet['text']
            de_words = tweet['de_words']
            tr_words = tweet['tr_words']
            self.show(text, de_words, tr_words)

            input_str = raw_input()
            # simple mark

            while input_str == 'h':
                print "Symbol\tMeaning"
                for (symbol, meaning) in HELPER:
                    print '%s\t%s' % (symbol, meaning)
                print 
                input_str = raw_input()

            if input_str in ['s', 'c', 'g', 't', 'n', 'p', 'x']:
                flag = input_str
                self.mark(oid, flag)

            else:
                print 'invalid annotation'


    # show highlighted text
    def show(self, text, de_words, tr_words):
        print '=' * 60
        for word in de_words:
            text = re.sub(r'(?<!%s)(%s)(?!%s)'%(ALPHABET, word, ALPHABET), color.RED + r'\1' + color.END, text)
        for word in tr_words:
            text = re.sub(r'(?<!%s)(%s)(?!%s)'%(ALPHABET, word, ALPHABET), color.CYAN + r'\1' + color.END, text)            
        print text.encode('utf-8')


    def mark(self, oid, flag):
        self.count += 1
        if flag not in self.history:
            self.history[flag] = 0
        self.history[flag] += 1
        self.tweets.update({'_id': oid}, {'$set': {'annotations.%s'%self.user: flag}}, upsert = True)


if __name__ == '__main__':
    db = 'code-switch'
    if len(sys.argv) != 2:
        print 'parameter: <username> e.g. annotator1'
        exit(0)
    user = sys.argv[1]
    annotator = Annotator(db, user)
    annotator.start()
