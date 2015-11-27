#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
from pymongo import MongoClient


def clear(user, db):
    input_str = raw_input('Are you sure to clear all the annotations by %s? [y/(n)]\n' % user)
    if input_str not in ['y', 'Y']:
        print 'Nothing happend!'
        exit(0)

    client = MongoClient('mongodb://%s:%s@ds049538.mongolab.com:49538/%s' % (user, user, db))
    tweets = client[db]['tweets']

    tweets.update({}, {'$unset': {'annotations.%s' % user: 1}}, multi = True)
    print 'All clear!'

if __name__ == '__main__':
    db = 'code-switch'
    user = sys.argv[1]
    clear(user, db)