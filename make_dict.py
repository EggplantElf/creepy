#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
# TODO:
# numeral-alphabet only



def simple_make(dict_file, freq_file, min_freq):
    d = set()
    for line in open(freq_file):
        tmp = line.strip().split()
        word, freq = tmp[0], int(tmp[1])
        if freq >= min_freq:
            d.add(word)
    # return d
    f = open(dict_file, 'w')
    for w in sorted(d):
        f.write(w + '\n')
    f.close()


# example: words in freq_de with freq >= 5 but not in freq_en with freq >= 10
def diff_make(dict_file, target_freq_file, subst_freq_file, min_target_freq, min_subst_freq):
    d, s = set(), set()
    for line in open(subst_freq_file):
        tmp = line.strip().split()
        word, freq = tmp[0], int(tmp[1])
        if freq >= min_subst_freq:
            s.add(word)

    for line in open(target_freq_file):
        tmp = line.strip().split()
        word, freq = tmp[0], int(tmp[1])
        if freq >= min_subst_freq and word not in s:
            d.add(word)
    # return d
    f = open(dict_file, 'w')
    for w in sorted(d):
        f.write(w + '\n')
    f.close()


if __name__ == '__main__':
    if len(sys.argv) == 4:
        simple_make(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    elif len(sys.argv) == 6:
        diff_make(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5]))
    else:
        print 'Wrong argument'


