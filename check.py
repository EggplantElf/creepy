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
# ALWAYS: decode().lower().encode()
# encapsule the lower() and title()


# filter @username, #topic and url
filter_pattern = re.compile(r'(@|#|https?:)\S*')

# try to rule out number, punctuation, proper noun, guess, abbreviation, and weird composition
# use regex to catch all
pattern1 = re.compile(r'_|<\+PUNCT>|<\+CARD>|<\+SYMBOL>|<\+NPROP>|<GUESSER>|<\^ABBR>')
pattern2 = re.compile(r'<NN>|<V>|<SUFF>|<VPART>') # check the compound words in dictionary, since morph are too loose


def read_dict(dict_file):
    d = set()
    for line in open(dict_file):
        d.add(line.strip())
    return d

class Checker:
    """
    for the speed of morphological analyzer, it has to work in batch,
    it analyzes e.g. every 1000 tweets in one go, then reloads the analyzer
    """

    def __init__(self, source_db, target_db, de_dict_file, tr_dict_file, policy):
        self.client = MongoClient()
        self.source_db = self.client[source_db]
        self.target_db = self.client[target_db]
        self.de_dict = read_dict(de_dict_file)
        self.tr_dict = read_dict(tr_dict_file)
        self.policy = policy


    # generator for reading tweets from db
    def tweet_stream(self):
        for tweet in self.source_db['tweets'].find():
            yield (tweet['text'], tweet['tweet_id'], tweet['user_id']) 


    def check(self):
        batch_num = 1
        size = 10000
        # for each batch
        for batch in self.batch(self.tweet_stream(), size):
            print batch_num * size 
            batch_num += 1
            words, counts = self.tokenize(batch)
            trs = self.check_tr(words)
            des = self.check_de(words)
            i = 0
            # ans = []
            for ((text, tid, uid), count) in zip(batch, counts):
                tr = trs[i: i + count] # [True, False, False, True]
                de = des[i: i + count] # [False, True, False, True]
                ws = words[i: i + count] # [tr, de, xx, tr]
                i += count

                de_list = [w for (w, d, t) in zip(ws, de, tr) if d and not t]
                if de_list and tr.count(True) >= tr.count(False):
                     self.log(text, tid, uid, de_list)



    # for debugging
    def check_single(self, text):
        text = text.decode('utf-8')
        words, counts = self.tokenize([(text, 'tid', 'uid')])
        print words
        # print counts
        tr = self.check_tr(words)
        print tr
        
        de = self.check_de(words)
        print de
        de_list = [w for (w, d, t) in zip(words, de, tr) if d and not t]
        print de_list
        if de_list and tr.count(True) >= 5:
             # self.log(text, tid, uid, de_list)
             print 'find one'




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
                                 {'$inc': {'count': 1},\
                                  '$set': {'searched': False}}, upsert = True)

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
                    words.append(token.lower().encode('utf-8', 'ignore'))
                    i += 1
            counts.append(i)
        return words, counts



    # input: list of words in the batch (encoded)
    # output: list of whether each word is a turkish word (either in morph or dict)
    def check_tr(self, words):
        """
        morphological analysis for turkish words
        """
        input_str = '\n'.join(w.decode('utf-8').title().encode('utf-8') for w in words) + '\n'
        # cmd = './bin/lookup -d -q -f bin/checker.script'
        cmd = './bin/Morph-Pipeline/lookup -d -q -f bin/Morph-Pipeline/test-script.txt'
        lookup = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        output = lookup.communicate(input=input_str)[0]
        morphs = output.strip().split('\n\n')
        print morphs
        print len(words)
        assert len(morphs) == len(words)
        # true if not ends with '+?', no matter how many analysis for a word
        morph_ans = map(lambda x: not x.endswith('*UNKNOWN*'), morphs)
        # morph_ans = map(lambda x: not x.endswith('_?'), morphs)
        dict_ans = [w in self.tr_dict for w in words]
        return [any(pair) for pair in zip(morph_ans, dict_ans)]


    def check_de(self, words):
        """
        morphological analysis for german words, exclude punctuation and numbers
        """

        input_str = '\n'.join(w.decode('utf-8').title().encode('utf-8') for w in words) + '\n'
        cmd = './bin/_run_smor.sh 2> /dev/null'
        lookup = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        output = lookup.communicate(input=input_str)[0]
        morphs = output.strip().split('\n\n')
        assert len(morphs) == len(words)
        morph_ans = map(lambda (w, m): self.is_de_word(w, m), zip(words, morphs))
        return morph_ans



    def is_de_word(self, word, morph_str):
        if self.policy == 1:
            for line in morph_str.split('\n'):
                if pattern1.search(line):
                    return False
                elif pattern2.search(line):
                    return (word in self.de_dict)
            return True
        elif self.policy >= 2:
            if pattern1.search(morph_str):
                return False
            else:
                return word in self.de_dict


if __name__ == '__main__':
    # source_db = sys.argv[1]
    # target_db = sys.argv[2]
    # if len(sys.argv) == 4:
    #     policy = int(sys.argv[3]) # 1 for loose, 2 for strict, 3 for super strict
    # else:
    #     policy = 2
    # checker = Checker(source_db, target_db, 'dict_de.txt', 'dict_tr.txt', policy)
    # checker.check()
    checker = Checker('a', 'b', 'dict_de.txt', 'dict_tr.txt', 3)
    checker.check_single('RT @herzgegenkopf: Hast du kein Respekt vor mir, hab ich kein Respekt vor dir.')

