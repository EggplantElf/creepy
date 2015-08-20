#!/usr/bin/python
# -*- coding: utf-8 -*-


import tweepy
import time
import json
from pymongo import MongoClient
from segtok.segmenter import split_multi
from segtok.tokenizer import word_tokenizer, split_contractions
import subprocess



# generator for reading tweets from db
def tweet_stream():
    client = MongoClient()
    tweets = client['twitter']['tweets']
    users = client['twitter']['users']
    return tweets.find({'user_id': '239308610'})

def tweet_stream():
    return [u"@GarantiyeSor olmayın benden. das Ist ein Test."]


def check(text):
#    print text
    for word in tokenize(text):
        tr = tr_morph(word)
        if not tr:
            de = de_morph(word)
            if de:
                print word, 'de'
            else:
                print word, '?'
        else:
            print word, 'tr'

def tokenize(text):
    """
    tokenize the text and filter the @username (and punctuation, smiley ...), leave only words
    """
    at = False
    for sent in split_multi(text):
        for token in word_tokenizer(sent):
            if token == '@':
                at = True
                continue
            if at:
                at = False
                continue
            if punctuation(token):
                continue
            yield(token.encode('utf-8', 'ignore'))    


def punctuation(word):
    return False


def tr_morph(word):
    """
    morphological analysis for turkish, works word by word, must run in daemon mode, otherwise very slow
    """
    cmd = './bin/lookup  -d -q -f ./bin/checker.script'
    lookup = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    output = lookup.communicate(input=word)[0]
    morphs = output.strip().split('\n')
    return morphs[0].split('\t')[-1] != '+?'


def de_morph(word):
    """
    morphological analysis for german words
    """
    cmd = './bin/_run_smor.sh 2> /dev/null'
    lookup = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    output = lookup.communicate(input=word)[0]
    morphs = output.strip().split('\n')
    return morphs[0].split('\t')[2] not in ['_', '<+PUNCT>']



if __name__ == '__main__':
    for t in tweet_stream():
        # text = t['text']
        check(t)
