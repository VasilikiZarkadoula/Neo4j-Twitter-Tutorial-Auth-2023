import pandas as pd
from py2neo import Graph, Node
import math

df = pd.read_csv("C:/Users/Kiki/Documents/Tweeter/Tweeter/WDM1.csv", low_memory=False, nrows=2000)
# Connect to the Neo4j database
graph = Graph("bolt://localhost:7687")

# Create sets to store unique tags, urls and tweets
unique_tags = set()
unique_urls = set()
unique_tweets = set()
unique_users = set()

# Iterate over the rows in the DataFrame and add tags, urls, and tweets to the sets
for index, row in df.iterrows():
    # Get the tweets from the row
    tweets = []
    tweet = {}
    tweet["id"] = row.get(f"includes.tweets.{0}.id")
    if all(tweet.values()) and not all(math.isnan(v) for v in tweet.values()):  # only add non-empty and non-nan tweets
        for j in range(0,25):
            tag = row.get(f"includes.tweets.0.entities.hashtags.{j}.tag")
            if(tag is not None):
                tag = str(tag).lower()
                tagRel = graph.run("MATCH (h: Hashtag),(t: Tweet) WHERE h.tag = $tag1 and t.id = $tweetId CREATE (t)-[r:HAS_HASHTAG]->(h) return count(r)",tag1 = tag, tweetId = tweet["id"]).data()
        
        for j in range(0,3):
            url = row.get(f"includes.tweets.0.entities.urls.{j}.expanded_url")
            if(url is not None):
                urlRel = graph.run("MATCH (l: Link),(t: Tweet) WHERE l.url = $url1 and t.id = $tweetId CREATE (t)-[r:HAS_URL]->(l) return count(r)",url1 = url, tweetId = tweet["id"]).data()


    # Get the users from the row
    users = []
    user = {}
    user["id"] = row.get(f"includes.users.{0}.id")

    if all(user.values()) and not all(math.isnan(v) for v in user.values()):  # only add non-empty and non-nan tweets
        for j in range(0,12):
            tag = row.get(f"includes.users.0.entities.description.hashtags.{j}.tag")
            if(tag is not None):
                tag = str(tag).lower()
                tagRel = graph.run("MATCH (h: Hashtag),(u: User) WHERE h.tag = $tag1 and u.id = $userId CREATE (u)-[r:USED_HASHTAG]->(h) return count(r)",tag1 = tag, userId = user["id"]).data()

        for j in range(0,1):
            url = row.get(f"includes.users.0.entities.description.urls.{j}.expanded_url")
            if(url is not None):
                urlRel = graph.run("MATCH (l: Link),(u: User) WHERE l.url = $url1 and u.id = $userId CREATE (u)-[r:USED_URL]->(l) return count(r)",url1 = url, userId = user["id"]).data()

        for j in range(0,5):
            mentions_username = row.get(f"data.entities.mentions.{j}.username")
            mentions_id = row.get(f"data.entities.mentions.{j}.id")
            if(mentions_username is not None):
                node = Node("User", id=mentions_id, username=mentions_username)
                graph.create(node)
                mentionRel = graph.run("MATCH (u1: User),(u2: USER) WHERE u1.id = $user1Id and u2.id = $user2Id CREATE (u1)-[r:MENTIONS]->(u2) return count(r)",user1Id = user["id"], user2Id = mentions_id).data()

graph.run("MATCH (u: User),(t: Tweet) WHERE u.id = t.authors_id and t.type = NULL CREATE (u)-[r:TWEETED]->(t) return count(r)")

graph.run("MATCH (u: User),(t: Tweet) WHERE u.id = t.authors_id and t.type = 'RETWEET' CREATE (u)-[r:RETWEETED]->(t) return count(r)")

graph.run("MATCH (u: User),(t: Tweet) WHERE u.id = t.authors_id and t.type = 'QUOTE' CREATE (u)-[r:QUOTED]->(t) return count(r)")

graph.run("MATCH (u: User),(t: Tweet) WHERE u.id = t.authors_id and t.type = 'REPLIED_TO' CREATE (u)-[r:REPLIED_TO]->(t) return count(r)")