import pyspark as ps
import pymongo as pm
from bson.json_util import dumps

conf=ps.SparkConf().setAppName('WordCount').setMaster('spark://ip-172-31-83-78.ec2.internal:7077')
sc=ps.SparkContext(conf=conf)

client = pm.MongoClient("mongodb://ec2-54-172-11-17.compute-1.amazonaws.com:27017")
db=client.tweetdb

news_result=dumps(db.news.find())
tweet_result=dumps(db.tweets.find())

input= open("input.txt","w+")
input.write(news_result)
input.write(tweet_result)

keywords=["education","canada","university","dalhousie", "expesive","good school","good schools","bad school","bad school", "poor school", "poor schools", "faculty","computer science","graduate"]

frequency={"education":0 ,"canada":0 ,"university":0 ,"dalhousie":0 , "expesive":0 ,"good school":0 ,"good schools":0 ,"bad school":0 ,"bad school":0 , "poor school":0 , "poor schools":0 , "faculty":0 ,"computer science":0 ,"graduate":0 }


words = sc.textFile("input.txt").flatMap(lambda line: line.split(" "))
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x1, x2: x1 + x2)
list1 = wordCounts.collect()
for keyword in keywords:
    for item in list1:
        if item[0].lower()==keyword:
             frequency[keyword]=frequency[keyword]+item[1]

output=open("output.txt","w")
print(frequency)
output.write(str(frequency))