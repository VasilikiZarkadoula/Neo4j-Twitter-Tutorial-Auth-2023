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
from py2neo import Graph, Node,  Relationship
import math

df = pd.read_csv("WDM1.csv", low_memory=False)

# Connect to the Neo4j database
graph = Graph("bolt://localhost:7687", auth=("neo4j", "twitterdb"))

# Create sets to store unique tags, urls and tweets
unique_tags = set()
unique_urls = set()
unique_tweets = set()
unique_users = set()

# Iterate over the rows in the DataFrame and add tags, urls, and tweets to the sets
for index, row in df.iterrows():

    # Get the tags from the row
    tweet_tags = [row.get(f"data.entities.hashtags.{i}.tag") for i in range(0, 25)]
    user_tags = [row.get(f"includes.users.0.entities.description.hashtags.{i}.tag") for i in range(0, 13)]

    # Get the urls from the row
    tweet_urls = [row.get(f"data.entities.urls.{i}.expanded_url") for i in range(0, 3)]
    user_urls = [row.get(f"includes.users.0.entities.description.urls.{i}.expanded_url") for i in range(0, 2)]

    # Get the tweets from the row
    tweet = dict()
    tweet["id"] = row.get(f"includes.tweets.0.id")
    tweet["created_at"] = row.get(f"includes.tweets.0.created_at")
    tweet["reply_count"] = row.get(f"includes.tweets.0.public_metrics.reply_count")
    tweet["type"] = row.get(f"includes.tweets.0.referenced_tweets.0.type")
    tweet["author_id"] = row.get(f"includes.tweets.0.author_id")

    # Get the users from the row
    user = dict()
    user["id"] = row.get(f"includes.users.0.id")
    user["username"] = row.get(f"includes.users.0.username")
    user["followers_count"] = row.get(f"includes.users.0.public_metrics.followers_count")

    # Remove any None or NaN values from the list of tags and urls
    tweet_tags = [str(tag).lower() for tag in tweet_tags if isinstance(tag, str)]
    user_tags = [str(tag).lower() for tag in user_tags if isinstance(tag, str)]

    tweet_urls = [str(url) for url in tweet_urls if isinstance(url, str)]
    user_urls = [str(url) for url in user_urls if isinstance(url, str)]


    # Add the lists to the sets
    unique_tags.update(tweet_tags)
    unique_tags.update(user_tags)

    unique_urls.update(tweet_urls)
    unique_urls.update(user_urls)

    # Convert the dictionary to a frozenset before adding to the set to make it hashable
    unique_tweets.add(frozenset(tweet.items()))
    unique_users.add(frozenset(user.items()))


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
    node = Node("Tweet", id=tweet_dict["id"], created_at=tweet_dict["created_at"], reply_count=tweet_dict["reply_count"]
                , type=tweet_dict["type"], author_id=tweet_dict["author_id"])
    graph.create(node)

# Create a node for each user
for user in unique_users:
    # Convert the hashable tuple back to a dictionary
    user_dict = dict(user)
    node = Node("User", id=user_dict["id"], username=user_dict["username"], followers_count=user_dict["followers_count"])
    graph.create(node)





