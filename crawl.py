import tweepy
import json
import pymongo

# This is the listener, resposible for receiving data
class StdOutListener(tweepy.StreamListener):
    def on_data(self, data):
        # Twitter returns data in JSON format - we need to decode it first
        decoded = json.loads(data)

        print '@%s: %s' % (decoded['user']['screen_name'].encode('utf-8', 'ignore'), decoded['text'].encode('utf-8', 'ignore'))
        print ''
        return True

    def on_error(self, status):
        print status

def read_auth_file(filename):
    data = open(filename).read()
    infos = json.loads(data)
    consumer_key = infos['consumer_key']
    consumer_secret = infos['consumer_secret']
    access_token = infos['access_token']
    access_token_secret = infos['access_token_secret']
    return consumer_key, consumer_secret, access_token, access_token_secret




if __name__ == '__main__':
    l = StdOutListener()
    consumer_key, consumer_secret, access_token, access_token_secret = read_auth_file('agent.pwd')
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    print "Stream:"
    stream = tweepy.Stream(auth, l)
    stream.filter(track=['n', 'r'], languages=['tr'])