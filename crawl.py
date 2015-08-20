import tweepy
import json
import sys
from pymongo import MongoClient
from pprint import pprint
from time import sleep, time

# TODO:
# try multi-thread, maybe not work 
# try to store with elasticsearch
# schedule to run it every day


# This is the listener, resposible for receiving data
class StdOutListener(tweepy.StreamListener):
    def __init__(self, client, limit):
        super(StdOutListener, self).__init__()
        self.tweets =  client['twitter']['tweets']
        self.users =  client['twitter']['users']
        self.count = 0
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
                place = tweet['place']
                country = place['country_code'] if place else None

                if original:
                    print text
                    self.tweets.insert({'tweet_id': tweet_id, \
                                        'text': text, \
                                        'user_id': user_id, \
                                        'country': country
                                        })

                    # if not self.users.find_one({'user_id':user_id}):
                        # self.users.insert({'user_id': user_id})

                    self.count += 1
                    if self.count >= self.limit:
                        return False
            except:
                pprint(tweet)
                raise KeyError
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
    consumer_key, consumer_secret, access_token, access_token_secret = read_auth_file('agent.pwd')
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    client = MongoClient()
    if len(sys.argv) == 2:
        limit = int(sys.argv[1]) 
    else:
        limit = 1000000

    l = StdOutListener(client, limit)
    t = time()
    stream = tweepy.Stream(auth, l)
    stream.filter(track=['n', 'r'], languages=['tr'])
    print time() - t



