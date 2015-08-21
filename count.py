from pymongo import MongoClient

def count():
    client = MongoClient()
    en = client['twitter_en']['tweets']
    de = client['twitter_de']['tweets']
    tr = client['twitter_tr']['tweets']
    print 'en:', en.count()
    print 'de:', de.count()
    print 'tr:', tr.count()



if __name__ == '__main__':
    count()