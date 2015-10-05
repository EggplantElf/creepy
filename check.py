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

# NOTE:
# Title() for morph analysis, lower() for dictionary lookup


# filter @username, #topic and url
filter_pattern = re.compile(r'(@|#|https?:)\S*')



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
        for batch in self.batch(self.tweet_stream(), size):
            print batch_num * size 
            batch_num += 1
            words, counts = self.tokenize(batch)
            trs = self.morph_tr(words)
            des = self.morph_de(words)
            i = 0
            # ans = []
            exit(0)
            for ((text, tid, uid), count) in zip(batch, counts):
                tr = trs[i: i + count] # [True, False, False, True]
                de = des[i: i + count] # [False, True, False, True]
                ws = words[i: i + count] # [tr, de, xx, tr]
                i += count
                # is_switch = any((not t and d) for (t, d) in zip(tr, de)) and tr.count(True) >= tr.count(False)
                # ans.append((is_switch, tr, de)) # (True, [True, False, False, True], [False, True, False, True])

                # new
                de_list = [w for (w, d, t) in zip(ws, de, tr) if d and not tr]
                if de_list and tr.count(True) >= tr.count(False):
                     self.log(text, tid, uid, de_list)

            # for (text, tid, uid), (a, tr, de) in zip(batch, ans):
            #     if a:
            #         de_list = [w for (w, d) in zip(ws, de) if d]
            #         self.log(text, tid, uid, de_list)


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

    # DONE
    # input decoded text
    # output incoded wordlist, lowercase
    def tokenize(self, tweets):
        """
        tokenize the text and filter the @username (and punctuation, smiley ...), leave only words
        """

        counts = [] # [5, 12, 0, 3, ...] the counts of valid words for each tweet
        words = [] # list of words
        # out = '' # one-word-per-line string of the tokenized words for morph analysis
        
        for (text, tid, uid) in tweets:
            i = 0
            text = filter_pattern.sub(' ', text)
            for sent in split_multi(text):
                for token in word_tokenizer(sent):
                    words.append(token.encode('utf-8', 'ignore').lower())
                    i += 1
            counts.append(i)
        return words, counts



    # DONE
    # input: list of words in the batch (encoded)
    # output: list of whether each word is a turkish word (either in morph or dict)
    def morph_tr(self, words):
        """
        morphological analysis for turkish words
        """

        input_str = '\n'.join(w.title() for w in words) + '\n'
        cmd = './bin/lookup -d -q -f bin/checker.script'
        lookup = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        output = lookup.communicate(input=input_str)[0]
        morphs = output.strip().split('\n\n')
        assert len(morphs) == len(words)
        # true if not ends with '+?', no matter how many analysis for a word
        morph_ans = map(lambda x: x.endswith('+?'), morphs)
        dict_ans = [w in self.tr_dict for w in words]
        return [any(pair) for pair in zip(morph_ans, dict_ans)]

    # DONE
    # input: list of words in the batch
    # output: list of whether each word is a german word (either in morph or dict)
    def morph_de(self, words):
        """
        morphological analysis for german words, exclude punctuation and numbers
        """

        input_str = '\n'.join(w.title() for w in words) + '\n'
        cmd = './bin/_run_smor.sh 2> /dev/null'
        lookup = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        output = lookup.communicate(input=input_str)[0]
        morphs = output.strip().split('\n\n')
        assert len(morphs) == len(words)
        # 
        morph_ans = map(lambda x: x.split('\t')[2] not in ['_', '<+PUNCT>', '<+CARD>', '<+SYMBOL>'], morphs)
        dict_ans = [w in self.de_dict for w in words]
        return [any(pair) for pair in zip(morph_ans, dict_ans)]




if __name__ == '__main__':
    source_db = sys.argv[1]
    target_db = 'new_switch'
    checker = Checker(source_db, target_db, 'freq_de.txt', 'freq_tr.txt', 5)
    checker.check()
