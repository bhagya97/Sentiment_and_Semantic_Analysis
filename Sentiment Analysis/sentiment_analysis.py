import pymongo as pm
import pandas as pd

client= pm.MongoClient("mongodb://127.0.0.1:27017")
db=client.tweetdb

# class to store the tweet information
class TweetInfo:

    def __init__(self,index,text,match,polarity):
        self.index=index
        self.text=text
        self.match=match
        self.polarity=polarity

    def display(self):
        result= (str)(self.index)+"\t"+self.text+"\t"+self.match+"\t"+self.polarity+"\n"
        return result
        
index=1

# open required files
bag_of_words=open("bag_of_words.txt","w")
output=open("output.txt","w")
output.flush()

positive=open("positive-words.txt","r")
positive_words=positive.readlines()

negative=open("negative-words.txt","r")
negative_words=negative.readlines()

# find polarity of words
tweets=db.tweets.find()
tweetInfo=None
for tweet in tweets:
    text=tweet['text'].strip()
    text=text.replace("\n", " ")
    words=text.split(" ")
    dict1={}
    for word in words:
        tweetInfo=None
        for pword in positive_words:
            if(word.strip()==pword.strip()):
                tweetInfo=TweetInfo(index,text.strip(),pword.strip(),"Positive")
                break;
        for nword in negative_words:
            if(word.strip()==nword.strip()):
                tweetInfo=TweetInfo(index,text.strip(),nword.strip(),"Negative")
                break;
        dict1.update({word:index})
        if tweetInfo is not None:
            output.write(tweetInfo.display())
    bag_of_words.write(str(dict1))
    bag_of_words.write("\n")
    index=index+1

output.close()
bag_of_words.close()

df = pd.read_csv("output.txt", delimiter='\t')
df.to_csv('sentiment_output.csv', header=['# Tweet','Message','Match','Polarity'])
print("Output written in sentiment_output.csv")