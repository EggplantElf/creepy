#!/usr/bin/python
# -*- coding: utf-8 -*-

from collections import defaultdict
import re
import sys

number = re.compile(r'\d+((,|.|/)\d*)*(%)?') # number, date

# very strict match of english, german and turkish words
alphabet = re.compile(r'^[a-zA-ZßäöüÄÖÜçÇşŞğĞıİ]+$')


def load(freq_file):
    d = defaultdict(int)
    try:
        for line in open(freq_file):
            tmp = line.decode('utf-8').strip().split()
            d[tmp[0]] = int(tmp[1])
    except:
        pass
    return d

def is_word(text):
    return alphabet.match(text.encode('utf-8'))

def read_freq(top = 10000):
    en_freq = load('freq_en.txt')
    de_freq = load('freq_de.txt')
    tr_freq = load('freq_tr.txt')

    # en_set = set(w for w, c in en_freq.iteritems() if c >= 5)
    # de_set = set(w for w, c in de_freq.iteritems() if c >= 5)
    # tr_set = set(w for w, c in tr_freq.iteritems() if c >= 5)

    en_set = set(w for w,c in sorted(en_freq.iteritems(), key=lambda (k, v): v, reverse = True)[:top])
    de_set = set(w for w,c in sorted(de_freq.iteritems(), key=lambda (k, v): v, reverse = True)[:top])
    tr_set = set(w for w,c in sorted(tr_freq.iteritems(), key=lambda (k, v): v, reverse = True)[:top])

    # print len(en_set)
    # print len(de_set)
    # print len(tr_set)

    de_pure = filter(is_word, de_set - en_set)
    tr_pure = filter(is_word, tr_set - en_set)

    # print len(de_pure)
    # print len(tr_pure)
    # print sorted(de_pure)[:100]

    f = open('dict_de.txt', 'w')
    for w in sorted(de_pure):
        f.write(w.encode('utf-8') + '\n')
    f.close()

    f = open('dict_tr.txt', 'w')
    for w in sorted(tr_pure):
        f.write(w.encode('utf-8') + '\n')
    f.close()



if __name__ == '__main__':
    if len(sys.argv) != 2:
        top = 100000
    else:
        top = int(sys.argv[1])
    read_freq(top)