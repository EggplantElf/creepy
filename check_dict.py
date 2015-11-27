#!/usr/bin/python
# -*- coding: utf-8 -*-


import tweepy
import time
import json
import re
import sys
import atexit
from pymongo import MongoClient
from segtok.segmenter import split_multi
from segtok.tokenizer import word_tokenizer, split_contractions
# from collections import defaultdict


# filter @username, #topic and url
filter_pattern = re.compile(r'(@|#|https?:)\S*')


def read_dict(freq_file):
    # d = defaultdict(int)
    d = {}
    try:
        for line in open(freq_file):
            items = line.strip().split()
            w, f = items[0], int(items[1])
            d[w] = f
    except:
        print '%s not exist' % freq_file
    return d

class Checker:

    def __init__(self, source_db, target_db, 
                de_dict_file, tr_dict_file, en_orig_dict_file,
                de_orig_dict_file, tr_orig_dict_file):
        self.source_client = MongoClient()
        self.source_db = self.source_client[source_db]
        self.target_client = MongoClient('mongodb://admin:admin@ds049538.mongolab.com:49538/%s' %target_db)
        self.target_db = self.target_client[target_db]
        self.de_dict = read_dict(de_dict_file)
        self.tr_dict = read_dict(tr_dict_file)
        self.en_orig_dict = read_dict(en_orig_dict_file)
        self.de_orig_dict = read_dict(de_orig_dict_file)
        self.tr_orig_dict = read_dict(tr_orig_dict_file)
        self.de_cache = read_dict('tmp/cache_de.txt')
        self.tr_cache = read_dict('tmp/cache_tr.txt')
        self.xx_cache = read_dict('tmp/cache_xx.txt')
        atexit.register(self.save_cache)

    def save_cache(self):
        print 'saving cache...'

        de = open('tmp/cache_de.txt', 'w')
        for (w, f) in sorted(self.de_cache.items(), key=lambda x: x[1], reverse=True):
            de.write('%s\t%d\n' %(w, f))
        de.close()

        tr = open('tmp/cache_tr.txt', 'w')
        for (w, f) in sorted(self.tr_cache.items(), key=lambda x: x[1], reverse=True):
            tr.write('%s\t%d\n' %(w, f))
        tr.close()

        xx = open('tmp/cache_xx.txt', 'w')
        for (w, f) in sorted(self.xx_cache.items(), key=lambda x: x[1], reverse=True):
            xx.write('%s\t%d\n' %(w, f))
        xx.close()


    # generator for reading tweets from db
    def tweet_stream(self):
        for tweet in self.source_db['tweets'].find({'status':0}):
            # change the state of the tweet as indexed (checked)
            # self.source_db['tweets'].update({'_id': tweet['_id']}, {'$set': {'status': 1}}, upsert = True)
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
            # if 'de' in codes:
            if self.is_swtich(codes):
                print '-' * 20
                print text.encode('utf-8')
                print '\033[91m' + ' '.join([w for w, c in zip(words, codes) if c == 'de']) + '\033[0m'
                print '\033[96m' + ' '.join([w for w, c in zip(words, codes) if c == 'tr']) + '\033[0m'
                # print '\033[93m' + ' '.join([w for w, c in zip(words, codes) if c == 'xx']) + '\033[0m'

                de_list = [w for w, c in zip(words, codes) if c == 'de']
                tr_list = [w for w, c in zip(words, codes) if c == 'tr']
                self.log(text, tid, uid, de_list, tr_list)

    def code(self, word):
        # check cache first
        if word in self.de_cache:
            self.de_cache[word] += 1
            return 'de'
        elif word in self.tr_cache:
            self.tr_cache[word] += 1
            return 'tr'
        elif word in self.xx_cache:
            self.xx_cache[word] += 1
            return 'xx'

        # word = word.decode('utf-8').lower().encode('utf-8')
        fde = self.de_dict[word] if word in self.de_dict else 1
        ftr = self.tr_dict[word] if word in self.tr_dict else 1
        fdeo = self.de_orig_dict[word] if word in self.de_orig_dict else 1
        ftro = self.tr_orig_dict[word] if word in self.tr_orig_dict else 1
        feno = self.en_orig_dict[word] if word in self.en_orig_dict else 1
        # print fde, ftr, fdeo, ftro, feno
        if fde > max(ftro * 5, feno * 5, 100) and ftr < 10 and feno < 100: # ftro < 10 ?
            self.de_cache[word] = 1
            return 'de'
        elif ftr > max(fdeo * 5, feno * 5, 100) and fde < 10 and feno < 100: # fdeo < 10 ?
            self.tr_cache[word] = 1
            return 'tr'
        else:
            self.xx_cache[word] = 1
            return 'xx'


    def is_swtich(self, codes):
        tr = codes.count('tr')
        de = codes.count('de')
        total = len(codes)
        return (tr * 3 > total and de > 0) or (de * 3 > total and tr > 0)




    def log(self, text, tid, uid, de_list, tr_list):
        print text.encode('utf-8')
        print '[' + ', '.join(de_list) + ']'
        ################
        # log the tweet
        try:
            self.target_db['tweets'].insert({'tweet_id': tid,\
                                       # 'flag': 'x',\
                                       'user_id': uid,\
                                       'text': text,\
                                       'de_words': de_list,\
                                       'tr_words': tr_list,
                                       'annotations': {}})
        except:
            print 'exception! probably duplicated key!'
        # # log the user
        # self.target_db['users'].update({'user_id': uid},\
        #                          {'$inc': {'count': 1}}, upsert = True)

        # # log the german words
        # for word in de_list:
        #     self.target_db['words'].update({'word': word},\
        #                              {'$inc': {'count': 1}}, upsert = True)



if __name__ == '__main__':
    source_db = sys.argv[1]
    target_db = sys.argv[2]
    # checker = Checker(source_db, target_db, 'freq/freq_de_morph_alphabet.txt', 'freq/freq_tr_morph_alphabet.txt', 'freq/freq_en_orig.txt')
    print 'reading frequency files...'
    checker = Checker(source_db, target_db, \
            'freq/freq_de_morph_alphabet.txt', \
            'freq/freq_tr_morph_alphabet.txt', \
            'freq/freq_en_orig.txt',\
            'freq/freq_de_orig.txt',\
            'freq/freq_tr_orig.txt')
    print 'start checking...'
    checker.check()
