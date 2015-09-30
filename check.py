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
import pexpect
import subprocess


# filter @username, #topic and url
filter_pattern = r'(@|#|https?:)\S*'



def read_dict(dict_file, min_freq):
    d = set()
    for line in open(dict_file):
        tmp = line.strip().split()
        word = tmp[0]
        freq = int(tmp[1])
        if freq >= min_freq:
            d.add(word)
    return d


class Checker:
    """
    for the speed of morphological analyzer, it has to work in batch,
    it analyzes e.g. every 1000 tweets in one go, then reloads the analyzer
    """

    def __init__(self, source_db, target_db, de_dict_file, tr_dict_file, min_freq = 5):
        self.client = MongoClient()
        self.source_db = self.client[source_db]
        self.target_db = self.client[target_db]
        self.de_dict = read_dict(de_dict_file, min_freq)
        self.tr_dict = read_dict(tr_dict_file, min_freq)


    # generator for reading tweets from db
    def tweet_stream(self):
        for tweet in self.source_db['tweets'].find():
            yield (tweet['text'], tweet['tweet_id'], tweet['user_id']) 


    def check(self):
        """
        analyzes every german word as well, if too slow, then change to analyze only non-turkish words
        """
        batch_num = 1
        size = 1000
        # for each batch
        for tweets in self.batch(self.tweet_stream(), size):
            print batch_num * size 
            batch_num += 1
            words, counts = self.tokenize(tweets)
            wlist = words.strip().split()
            trs = self.morph_tr(words)
            des = self.morph_de(words)
            i = 0
            ans = []
            for count in counts:
                tr = trs[i: i + count]
                de = des[i: i + count]
                ws = wlist[i: i + count]
                i += count
                is_switch = any((not t and d) for (t, d) in zip(tr, de)) and tr.count(True) >= tr.count(False)
                ans.append((is_switch, tr, de))

            for (text, tid, uid), (a, tr, de) in zip(tweets, ans):
                if a:
                    de_list = [w for (w, d) in zip(ws, de) if d]
                    self.log(text, tid, uid, de_list)


    def log(self, text, tid, uid, de_list):
        print text.encode('utf-8')
        print de_list
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



    def batch(self, stream, size):
        out = []
        i = 0
        for (text, tid, uid) in stream:
            out.append((text, tid, uid))
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
        
        for (text, tid, uid) in tweets:
            i = 0
            text = re.sub(filter_pattern, '', text)
            for sent in split_multi(text):
                for token in word_tokenizer(sent):
                    out += (token.encode('utf-8', 'ignore').title() + '\n') # Capitalized
                    i += 1
            counts.append(i)
        return out, counts

    # does it case sensitive?
    def morph_tr(self, words):
        """
        morphological analysis for turkish words
        """
        cmd = './bin/lookup -d -q -f bin/checker.script'
        lookup = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        output = lookup.communicate(input=words)[0]
        morphs = output.strip().split('\n\n')
        assert len(morphs) == len(words.strip().split('\n'))
        morph_ans = map(lambda x: x[-2:] != '+?', morphs)
        dict_ans = [w.lower() in self.tr_dict for w in words.strip().split()]
        return [any(pair) for pair in zip(dict_ans, morph_ans)]

    # already capitalized
    def morph_de(self, words):
        """
        morphological analysis for german words, exclude punctuation and numbers
        """
        cmd = './bin/_run_smor.sh 2> /dev/null'
        lookup = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        output = lookup.communicate(input=words)[0]
        morphs = output.strip().split('\n\n')
        assert len(morphs) == len(words.strip().split('\n'))
        morph_ans = map(lambda x: x.split('\t')[2] not in ['_', '<+PUNCT>', '<+CARD>', '<+SYMBOL>'], morphs)
        dict_ans = [w.lower() in self.de_dict for w in words.strip().split()]
        return [any(pair) for pair in zip(dict_ans, morph_ans)]

if __name__ == '__main__':
    source_db = sys.argv[1]
    target_db = 'new_switch'
    checker = Checker(source_db, target_db, 'freq_de.txt', 'freq_tr.txt', 5)
    checker.check()
