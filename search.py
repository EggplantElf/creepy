# from tweepy import OAuthHandler
import tweepy
import json

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

    api = tweepy.API(auth, wait_on_rate_limit=True)

    for tweet in api.user_timeline(user_id='100020219', count=200): #maximum count = 200
        print tweet.text
        print '------'