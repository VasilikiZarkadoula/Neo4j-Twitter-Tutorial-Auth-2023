'''
import pymongo
client = pymongo.MongoClient(port=27017)
# Database Name
db = client["Project_I"]
# Collection Name
col = db["WDM1"]
x = col.find()
data = [item for item in x]
print(data)
client.close()
'''

import pandas as pd
from py2neo import Graph, Node

df = pd.read_csv("WDM1.csv", low_memory=False)
#print(df.head())

# Connect to the Neo4j database
graph = Graph("bolt://localhost:7687", auth=("neo4j", "twitterdb"))

# Create sets to store unique tags, urls and tweets
unique_tags = set()
unique_urls = set()
unique_tweets = set()

# Iterate over the rows in the DataFrame and add tags, urls, and tweets to the sets
for index, row in df.iterrows():
    # Get the tags from the row
    tags = [row.get(f"data.entities.hashtags.{i}.tag") for i in range(0, 25)]
    urls = [row.get(f"data.entities.urls.{i}.expanded_url") for i in range(0, 3)]
    tweet = {
        "id": row["data.id"],
        "created_at": row["data.created_at"],
        "reply_count": row["data.public_metrics.reply_count"]
    }
    '''
    range (0,4)
    includes.users.{i}.public_metrics.followers_count
    includes.users.{i}.username
    includes.users.{i}.id
    '''

    # Remove any None or NaN values from the list of tags and urls
    tags = [str(tag).lower() for tag in tags if isinstance(tag, str)]
    urls = [url for url in urls if url is not None]

    # Add the lists to the sets
    unique_tags.update(tags)
    unique_urls.update(urls)
    # Add the tweet as a hashable tuple to the set
    unique_tweets.add(tuple(sorted(tweet.items())))

# Create a node for each tag
for tag in unique_tags:
    node = Node("Hashtag", tag=tag)
    graph.create(node)

# Create a node for each url
for url in unique_urls:
    node = Node("Link", url=url)
    graph.create(node)

# Create a node for each tweet
for tweet in unique_tweets:
    # Convert the hashable tuple back to a dictionary
    tweet_dict = dict(tweet)
    node = Node("Tweet", id=tweet_dict["id"], created_at=tweet_dict["created_at"], reply_count=tweet_dict["reply_count"])
    graph.create(node)