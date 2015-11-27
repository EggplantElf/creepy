#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re

alphabet = re.compile(r'^[a-zA-ZßäöüÄÖÜçÇşŞğĞıİ\-\'\.]+$')


def check(de_freq_file, en_freq_file, de_dict_file, ratio, min_de_freq, max_en_freq):
    de_dict = {}
    en_dict = {}
    de_words = []
    for line in open(de_freq_file):
        items = line.strip().split()
        w, f = items[0], int(items[1])
        de_dict[w] = f
        de_words.append(w)
    for line in open(en_freq_file):
        items = line.strip().split()
        w, f = items[0], int(items[1])
        en_dict[w] = f

    out = open(de_dict_file, 'w')

    for w in de_words:
        if alphabet.match(w):
            de_freq = de_dict[w]
            if de_freq >= min_de_freq:
                if w not in en_dict:
                    out.write('%s\t%d\n' % (w, de_freq))
                else:
                    en_freq = en_dict[w]
                    if (de_freq > en_freq * ratio and en_freq < max_en_freq):
                        out.write('%s\t%d\n' % (w, de_freq))
    out.close()


if __name__ == '__main__':
    de_freq_file = sys.argv[1]
    en_freq_file = sys.argv[2]
    de_dict_file = sys.argv[3]
    ratio = 5
    min_de_freq = 10
    max_en_freq = 50

    check(de_freq_file, en_freq_file, de_dict_file, ratio, min_de_freq, max_en_freq)