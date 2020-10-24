import numpy as np
import random
import re
from collections import defaultdict
from google.cloud import bigquery

client = bigquery.Client()

markov_graph = defaultdict(lambda: defaultdict(int))

def get_markov_graph():
    data = []

    query_sql = "SELECT * FROM `tanelis.markov_chain.markov_graph`"

    # SELECT
    # 'tämä' AS current_word,
    #     [STRUCT('on' AS word,
    #     20 AS count,
    #     20/100 AS weight),
    #     STRUCT('ei' AS word,
    #     80 AS count,
    #     80/100 AS weight)] AS next

    client = bigquery.Client()
    query_job = client.query(query_sql)
    results = query_job.result()

    for row in results:
        current_word = row["current_word"]
        next_array = row["next"]

        for obj in next_array:
            next_word = obj["word"]
            next_word_count = obj["count"]
            markov_graph[current_word][next_word] = next_word_count


get_markov_graph()
#print(markov_graph)

print('graph created')
# asfdfd


# def get_text_array():
#     data = []

#     query_sql = """
#     SELECT
#     word
#     FROM
#     `tanelis.tweets_eu.tweets_reporting`,
#     UNNEST(words) AS word
#     LIMIT 1000
#     """

#     client = bigquery.Client()
#     query_job = client.query(query_sql)
#     results = query_job.result()

#     for row in results:
#         data.append(row["word"])

#     return data


# tokenized_text = get_text_array()

# # print(tokenized_text)

# last_word = tokenized_text[0].lower()
# for word in tokenized_text[1:]:
#     word = word.lower()
#     markov_graph[last_word][word] += 1
#     last_word = word

# print(markov_graph)



# Preview graph.
# limit = 3
# for first_word in ('Vake', 'palkkasi'):
#   next_words = list(markov_graph[first_word].keys())[:limit]
#   for next_word in next_words:
#     print(first_word, next_word)


# TODO:
# Select the start word randomly based what have actually been common start words
# if a start word has not been specified in the function

# For x first words, e.g. 6, don't return any words that don't have a next word in the graph
# (stop words). After x words do some probabilistic determination on when it's a good
# time to stop. E.g. based on how often the word has ended a tweet. The longer the tweet
# is the more likely the word should be to end it.

def walk_graph(graph, distance=5, start_node=None):
    """Returns a list of words from a randomly weighted walk."""
    if distance <= 0:
        return []

    # If not given, pick a start node at random.
    if not start_node:
        start_node = random.choice(list(graph.keys()))

    weights = np.array(
        list(markov_graph[start_node].values()),
        dtype=np.float64)
    # Normalize word counts to sum to 1.
    weights /= weights.sum()

    # Pick a destination using weighted distribution.
    choices = list(markov_graph[start_node].keys())
    chosen_word = np.random.choice(choices, None, p=weights)

    return [chosen_word] + walk_graph(
        graph, distance=distance-1,
        start_node=chosen_word)


for i in range(10):
    print(' '.join(walk_graph(
        markov_graph, distance=12, start_node="tämä")), '\n')
