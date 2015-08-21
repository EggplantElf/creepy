#!/usr/bin/python
# -*- coding: utf-8 -*-


import tweepy
import time
import json
from pymongo import MongoClient
from segtok.segmenter import split_multi
from segtok.tokenizer import word_tokenizer, split_contractions
import pexpect
import subprocess




# generator for reading tweets from db
def tweet_stream():
    client = MongoClient()
    tweets = client['twitter']['tweets']
    users = client['twitter']['users']
#   for tweet in tweets.find({'user_id': '239308610'}):
    for tweet in tweets.find():
        yield tweet['text']


class Checker:
    """
    for the speed of morphological analyzer, it has to work in batch,
    it analyzes e.g. every 1000 tweets in one go, then reloads the analyzer
    """

    def __init__(self):
        pass

    def check(self, stream):
        """
        analyzes every german word as well, if too slow, then change to analyze only non-turkish words
        """
        for tweets in self.batch(stream):
            words, counts = self.tokenize(tweets)
            trs = self.morph_tr(words)
            des = self.morph_de(words)
            i = 0
            ans = []
            for count in counts:
                tr = trs[i: i + count]
                de = des[i: i + count]
                i += count
                is_switch = any((not t and d) for (t, d) in zip(tr, de)) and tr.count(True) >= tr.count(False)
                ans.append((is_switch, tr, de))

        for t,(a, tr, de) in zip(tweets, ans):
            if a:
                print t.encode('utf-8')
    #            print tr, de
        return ans

    def batch(self, stream, size = 1000):
        out = []
        i = 0
        for text in stream:
            out.append(text)
            i += 1
            if i == size:
                yield out
                out = []
                i = 0
        if out:
            yield out


    def tokenize(self, tweets):
        """
        tokenize the text and filter the @username (and punctuation, smiley ...), leave only words
        """

        counts = [] # [5, 12, 0, 3, ...] the counts of valid words for each tweet
        out = '' # one-word-per-line string of the tokenized words for morph analysis
        at = False
        
        for text in tweets:
            i = 0
            for sent in split_multi(text):
                for token in word_tokenizer(sent):
                    if token == '@':
                        at = True
                        continue
                    if at:
                        at = False
                        continue
                    out += (token.encode('utf-8', 'ignore').title() + '\n')
                    i += 1
            counts.append(i)
        return out, counts


    def morph_tr(self, words):
        """
        morphological analysis for turkish words
        """
        cmd = './bin/lookup -d -q -f bin/checker.script'
        lookup = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        output = lookup.communicate(input=words)[0]
        morphs = output.strip().split('\n\n')
        assert len(morphs) == len(words.strip().split('\n'))
        return map(lambda x: x[-2:] != '+?', morphs)


    def morph_de(self, words):
        """
        morphological analysis for german words, exclude punctuation and numbers
        """
        cmd = './bin/_run_smor.sh 2> /dev/null'
        lookup = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        output = lookup.communicate(input=words)[0]
        morphs = output.strip().split('\n\n')
        assert len(morphs) == len(words.strip().split('\n'))
        return map(lambda x: x.split('\t')[2] not in ['_', '<+PUNCT>', '<+CARD>', '<+SYMBOL>'], morphs)


if __name__ == '__main__':
    checker = Checker()
    checker.check(tweet_stream())
