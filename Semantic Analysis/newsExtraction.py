from newsapi import NewsApiClient
import pymongo as pm
import re

# MongoDB settings
client = pm.MongoClient("mongodb://ec2-54-172-11-17.compute-1.amazonaws.com:27017")
db=client.newsdb

##if "news" in db.collection_names():
##    db["news"].drop()

# news api call
apikey="3862fd6d63fd46e3936d7709e2df906a"
api = NewsApiClient(apikey)

search_keywords=["Canada", "University", "Dalhousie University", "Halifax", "Canada Education", "Moncton", "Toronto"]

for keyword in search_keywords:
    for i in range(0,21):
            
        news = api.get_everything(q=keyword)
        articles=news['articles']

        for article in articles:
            try:
                content=article['content']
                description=article['description']
                
                #remove emojis and url from description and urls
                content=content.encode('ascii', 'ignore').decode('ascii')
                content = re.sub(r"http\S+", "", content, re.MULTILINE)
                description=description.encode('ascii', 'ignore').decode('ascii')
                description = re.sub(r"http\S+", "", description, re.MULTILINE)
                
                news_data={ "Source" : article['source']['name']
                    , "Author" : article['author']
                    , "Title" : article['title']
                    , "Description" : description
                    , "Time" : article['publishedAt']
                    , "Content" : content
                    }
                db.news.insert_one(news_data)
                print(news_data)
            except KeyError:
                pass
