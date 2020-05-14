import tweepy as tp
import pymongo as pm
import json
import re
client = pm.MongoClient("mongodb://ec2-54-172-11-17.compute-1.amazonaws.com:27017")
#db=client.tweetdb
dblist=client.database_names()
if "tweetdb" in dblist:
    client.drop_database("tweetdb")
    
db=client.tweetdb

api_key="YOv5Y6Gm9bCb60XYN6PzLo7nS"
secret_key="4IeZ62pBnRY7r0rxN9H8hyQLwO2W08yCRtyTyMRSa6X3Z8tOS9"
access_token="1233888570403958790-l88SnT4tbjqRGrM8QPNnWrF7j5hIgl"
secret_token="DdFGEnoNmV0nHPo5ll3FGEcJorOK2vZk2z9hBTK3dlsy5"

authentication=tp.OAuthHandler(api_key,secret_key)
authentication.set_access_token(access_token,secret_token)
tweet_api=tp.API(authentication)

#results=tp.Cursor(tweet_api.search,q="Canada")
search_keywords=["Canada", "University", "Dalhousie University", "Halifax", "Canada Education"]
data={}
for keyword in search_keywords:
    for i in range(0,10):
        
        results=tweet_api.search(q=keyword,count=100)
        for result in results:

            try:
                text=result.text
                retweet=result._json['retweeted_status']['text']
                
                #remove emojis
                text=text.encode('ascii', 'ignore').decode('ascii')
                retweet=retweet.encode('ascii', 'ignore').decode('ascii')
                #remove urls
                text = re.sub(r"http\S+", "", text, re.MULTILINE)
                retweet = re.sub(r"http\S+", "", retweet, re.MULTILINE)

                data={'text': text,
                       'retweet': result._json['retweeted_status']['text'],
                       'location': result.user.location,
                       'time':result.created_at,
                       'language' : result.metadata['iso_language_code']
                       }
                db.tweets.insert_one(data)
            except KeyError:
                pass
