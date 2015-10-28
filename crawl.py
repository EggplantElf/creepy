#!/usr/bin/python
# -*- coding: utf-8 -*-

import tweepy
import json
import sys
from pymongo import MongoClient
from pprint import pprint
from time import sleep, time
import argparse

# TODO:
# try to store with elasticsearch
# add crawling date

# This is the listener, resposible for receiving data
class StdOutListener(tweepy.StreamListener):
    def __init__(self, client, limit, lang):
        super(StdOutListener, self).__init__()
        self.tweets =  client['new_'+lang]['tweets']
        self.users =  client['new_'+lang]['users']
        self.count = 0
        self.errors = 0
        self.limit = limit


    def on_data(self, data):
        """
        return False will stop the stream
        """
        tweet = json.loads(data)
        if 'limit' not in tweet:
            try:
                user_id = tweet['user']['id_str'].encode('utf-8', 'ignore')
                tweet_id = tweet['id_str']
                text = tweet['text'].encode('utf-8', 'ignore')
                original = 'retweeted_status' not in tweet 
                coordinates = tweet['coordinates']
                if coordinates:
                    coordinates = coordinates['coordinates']
                entities = tweet['entities']
                mentions = [m['id_str'] for m in entities['user_mentions']]
                names = [m['screen_name'].encode('utf-8', 'ignore') for m in entities['user_mentions']]
                hashtags = [t['text'].encode('utf-8', 'ignore') for t in entities['hashtags']]
                urls = [u['url'].encode('utf-8', 'ignore') for u in entities['urls']]

                # some thing to normalize the text
                # for n in names:
                #     text = text.replace('@' + n, '')
                # for h in hashtags:
                #     text = text.replace('#' + h, '')
                # for u in urls:
                #     text = text.replace(u, '')


                if original:
                    print text
                    # pprint(tweet)
                    # print '***************************'
                    self.tweets.insert({'tweet_id': tweet_id, \
                                        'text': text, \
                                        'user_id': user_id, \
                                        'coordinates': coordinates,\
                                        'mentions': mentions,\
                                        'hashtags': hashtags,\
                                        'urls': urls,\
                                        'status': 0 # can have arbitrary meanings
                                        })

                    # if not self.users.find_one({'user_id':user_id}):
                        # self.users.insert({'user_id': user_id})

                    self.count += 1
                    if self.count >= self.limit:
                        return False
            except:
                pprint(tweet)
                # raise KeyError
                self.errors += 1
                if self.errors > 10:
                    return False
                else:
                    sleep(1)
                    return True
        else:
            print 'limit'
            # sleep(1)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            return False


def read_auth_file(filename):
    data = open(filename).read()
    infos = json.loads(data)
    consumer_key = infos['consumer_key']
    consumer_secret = infos['consumer_secret']
    access_token = infos['access_token']
    access_token_secret = infos['access_token_secret']
    return consumer_key, consumer_secret, access_token, access_token_secret


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Crawler parameters')

    parser.add_argument('-l', action='store', dest='lang',
                        default='en',
                        help='language, default = en')

    parser.add_argument('-c', action='store', default=10000000,
                        dest='count', type=int,
                        help='number of tweets to crawl')
    param = parser.parse_args()


    consumer_key, consumer_secret, access_token, access_token_secret = read_auth_file('/home/users0/xiangyu/creepy/pwd/%s.pwd' % param.lang)
    # consumer_key, consumer_secret, access_token, access_token_secret = read_auth_file('pwd/%s.pwd' % param.lang)
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)



    client = MongoClient()
    l = StdOutListener(client, param.count, param.lang)
    t = time()
    stream = tweepy.Stream(auth, l)
    stream.filter(track=['a', 'e', 'i', 'o', 'u'], languages=[param.lang])
    print time() - t



