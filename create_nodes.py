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
    tags = [row.get(f"data.entities.hashtags.{i}.tag") for i in range(0, 25)]
    urls = [row.get(f"data.entities.urls.{i}.expanded_url") for i in range(0, 3)]
    # Get the tweets from the row
    tweets = []
    for i in range(0, 2):
        tweet = {}
        tweet["id"] = row.get(f"includes.tweets.{i}.id")
        tweet["created_at"] = row.get(f"includes.tweets.{i}.created_at")
        tweet["reply_count"] = row.get(f"includes.tweets.{i}.public_metrics.reply_count")
        if all(tweet.values()) and not all(math.isnan(v) for v in tweet.values()):  # only add non-empty and non-nan tweets
            tweets.append(tweet)

    # Get the users from the row
    users = []
    for i in range(0, 4):
        user = {}
        user["id"] = row.get(f"includes.users.{i}.id")
        user["username"] = row.get(f"includes.users.{i}.username")
        user["followers_count"] = row.get(f"includes.users.{i}.public_metrics.followers_count")
        if all(user.values()) and not all(math.isnan(v) for v in user.values()):  # only add non-empty and non-nan tweets
            users.append(user)

    # Remove any None or NaN values from the list of tags and urls
    tags = [str(tag).lower() for tag in tags if isinstance(tag, str)]
    urls = [url for url in urls if url is not None and url != "nan"]  # exei ena nan pou den exw kataferei na to diwksw

    # Add the lists to the sets
    unique_tags.update(tags)
    unique_urls.update(urls)
    # Add the tweets as a hashable tuple to the set
    unique_tweets.update([tuple(sorted(tweet.items())) for tweet in tweets])
    unique_users.update([tuple(sorted(user.items())) for user in users])


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

# Create a node for each user
for user in unique_users:
    # Convert the hashable tuple back to a dictionary
    user_dict = dict(user)
    node = Node("User", id=user_dict["id"], username=user_dict["username"], followers_count=user_dict["followers_count"])
    graph.create(node)
