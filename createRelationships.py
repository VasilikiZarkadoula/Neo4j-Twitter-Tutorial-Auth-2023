import pandas as pd
from py2neo import Graph, Node
import math

df = pd.read_csv("C:/Users/Paris/Documents/WebMining/WDM1.csv", low_memory=False)
# Connect to the Neo4j database
graph = Graph("bolt://localhost:7687")

# Iterate over the rows in the DataFrame and add tags, urls, and tweets to the sets
for index, row in df.iterrows():
    # Get the tweets from the row
    tweet = {}
    tweet["id"] = row.get(f"includes.tweets.{0}.id")
    if all(tweet.values()) and not all(math.isnan(v) for v in tweet.values()):  # only add non-empty and non-nan tweets
        for j in range(0,25):
            tag = row.get(f"includes.tweets.0.entities.hashtags.{j}.tag")
            if(tag != None):
                tag = str(tag).lower()
                tagRel = graph.run("MATCH (h: Hashtag),(t: Tweet) WHERE h.tag = $tag1 and t.id = $tweetId CREATE (t)-[r:HAS_HASHTAG]->(h) return count(r)",tag1 = tag, tweetId = tweet["id"]).data()
        print(tag + " tweet")
        for j in range(0,3):
            url = row.get(f"includes.tweets.0.entities.urls.{j}.expanded_url")
            if(url != None):
                urlRel = graph.run("MATCH (l: Link),(t: Tweet) WHERE l.url = $url1 and t.id = $tweetId CREATE (t)-[r:HAS_URL]->(l) return count(r)",url1 = url, tweetId = tweet["id"]).data()
        print(url + " tweet")

    # Get the users from the row
    user = {}
    user["id"] = row.get(f"includes.users.{0}.id")

    if all(user.values()) and not all(math.isnan(v) for v in user.values()):  # only add non-empty and non-nan tweets
        for j in range(0,13):
            tag = row.get(f"includes.users.0.entities.description.hashtags.{j}.tag")
            if(tag != None):
                tag = str(tag).lower()
                tagRel = graph.run("MATCH (h: Hashtag),(u: User) WHERE h.tag = $tag1 and u.id = $userId CREATE (u)-[r:USED_HASHTAG]->(h) return count(r)",tag1 = tag, userId = user["id"]).data()
        print(tag + " user")

        for j in range(0,2):
            url = row.get(f"includes.users.0.entities.description.urls.{j}.expanded_url")
            if(url != None):
                urlRel = graph.run("MATCH (l: Link),(u: User) WHERE l.url = $url1 and u.id = $userId CREATE (u)-[r:USED_URL]->(l) return count(r)",url1 = url, userId = user["id"]).data()
        print(url + " user")

        for j in range(0,6):
            mentions_username = row.get(f"data.entities.mentions.{j}.username")
            mentions_id = row.get(f"data.entities.mentions.{j}.id")
            if(mentions_username != None and mentions_id != None):
                node = Node("User", id=mentions_id, username=mentions_username)
                graph.create(node)
                mentionRel = graph.run("MATCH (u1: User),(u2: USER) WHERE u1.id = $user1Id and u2.id = $user2Id CREATE (u1)-[r:MENTIONS]->(u2) return count(r)",user1Id = user["id"], user2Id = mentions_id).data()
        print(mentions_id + " user")

print("tweet begin")
graph.run("MATCH (u: User),(t: Tweet) WHERE u.id = t.author_id and t.type<> 'retweeted' and t.type<> 'quoted' and t.type<> 'replied_to' CREATE (u)-[r:TWEETED]->(t) return count(r)")

print("retweet begin")
graph.run("MATCH (u: User),(t: Tweet) WHERE u.id = t.author_id and t.type = 'retweeted' CREATE (u)-[r:RETWEETED]->(t) return count(r)")

print("quote begin")
graph.run("MATCH (u: User),(t: Tweet) WHERE u.id = t.author_id and t.type = 'quoted' CREATE (u)-[r:QUOTED]->(t) return count(r)")

print("reply begin")
graph.run("MATCH (u: User),(t: Tweet) WHERE u.id = t.author_id and t.type = 'replied_to' CREATE (u)-[r:REPLIED_TO]->(t) return count(r)")
