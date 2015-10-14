#!/usr/bin/python
# -*- coding: utf-8 -*-

import tweepy
import json
import sys
from pymongo import MongoClient

# use unique index instead, should be much faster


def merge(target_db, add_db, collection, key):
    client = MongoClient()
    target = client[target_db][collection]
    add = client[add_db][collection]
    for item in add.find():
        if not target.find_one({key: item[key]}):
            target.insert(item)


if __name__ == '__main__':
    target_db = sys.argv[1]
    add_db = sys.argv[2]
    collection = sys.argv[3]
    key = sys.argv[4]
    merge(target_db, add_db, collection, key)