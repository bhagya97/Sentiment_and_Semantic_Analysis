import pymongo as pm
import pandas as pd
import math

client= pm.MongoClient("mongodb://127.0.0.1:27017")
db=client.newsdb

total=db.news.count()
news= db.news.find()

########### Part 1
# Process db data and convert them to files
count=1
for article in news:
    file_name="file"+str(count)+".txt"
    file= open(file_name,'w')
    title=article['Title']
    description=article['Description']
    content=article['Content']
    output="Title: "+title +"\nDescription: "+description+"\nContent: "+content;
    file.write(output)
    file.close()
    count=count+1
count=count-1
print("Total ", count , " articles processed" )


########### Part 2
# Derive the frequency count for each keyword
dict1={}
c1,c2,c3,c4=[],[],[],[]
keywords=["Canada","University", "Dalhousie", "Halifax", "Business"]

for keyword in keywords:

    frequency = 0
    for i in range(1,count):
        file_name="file"+str(i)+".txt"
        file= open(file_name,'r')
        result=file.read()
        words=result.split(" ")
        for word in words:
            word=word.strip()
            if(word.lower()==keyword.lower()):
                frequency=frequency+1
                break

    dict1.update({keyword:frequency})
    c1.append(keyword)
    c2.append(frequency)
    if(frequency!=0):
        c3.append(count/frequency)
        c4.append(math.log10(count/frequency))
    else:
        c3.append("N/A")
        c4.append("N/A")

df2 = pd.DataFrame({'Search Query': c1, 'Document Containing term(df)': c2, 'Total Documents(N)/Documents containing the term(df)': c3, 'Log10(N/df)':c4})
df2.to_csv("TFIDF.csv")
print("TF-IDF (term frequency-inverse document frequency) table generated")


########### Part 3
# frequency count for term "CANADA"
c1,c2,c3=[],[],[]
index=1
highest=0
for i in range(1, count):
    frequency=0
    file_name = "file" + str(i) + ".txt"
    file = open(file_name, 'r')
    result = file.read()
    words = result.split(" ")
    total_words=len(words)
    for word in words:
        word = word.strip()
        if (word.lower() == "canada"):
            frequency = frequency + 1
    if frequency>0:
        c1.append(index)
        c2.append(total_words)
        c3.append(frequency)
        index = index + 1
    relative_frequency=frequency/total_words
    highest=max(relative_frequency,highest)
    if highest==relative_frequency:
        article=result


df3 = pd.DataFrame({'Article #': c1, 'Total words': c2, 'frequency': c3})
df3.to_csv("Frequency_Table.csv")
print("Frequency table generated")
print("\n"+article)



