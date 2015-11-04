#!/usr/bin/python
# -*- coding: utf-8 -*-

from Collections import defaultdict


def make_list_from_csv(filename, lang):
    d = defaultdict(int)
    for line in open(filename):
        items = line.strip.split(',', 3)
        if items[0] == lang:
            print items[3]



if __name__ == '__main__':
    make_list_from_csv()