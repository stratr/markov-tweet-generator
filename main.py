import numpy as np
import random
import re
from collections import defaultdict
from google.cloud import bigquery

client = bigquery.Client()


def query_request(query_sql):
    data = []

    client = bigquery.Client()
    query_job = client.query(query_sql)
    results = query_job.result()

    for row in results:
        data.append(row["last_tweet_time"])

    return data


def query_word_array():
    data = []

    query_sql = """
    SELECT
    word
    FROM
    `tanelis.tweets_ml.training_data2`,
    UNNEST(words) AS word
    """

    client = bigquery.Client()
    query_job = client.query(query_sql)
    results = query_job.result()

    for row in results:
        data.append(row["word"])

    return data


tokenized_text = query_word_array()

# tokenized_text = [
#     word
#     for word in re.split('\W+', text)
#     if word != ''
# ]

# print(tokenized_text)

markov_graph = defaultdict(lambda: defaultdict(int))

print(markov_graph)

last_word = tokenized_text[0].lower()
for word in tokenized_text[1:]:
    word = word.lower()
    markov_graph[last_word][word] += 1
    last_word = word

# Preview graph.
# limit = 3
# for first_word in ('Vake', 'palkkasi'):
#   next_words = list(markov_graph[first_word].keys())[:limit]
#   for next_word in next_words:
#     print(first_word, next_word)


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
        markov_graph, distance=12)), '\n')
