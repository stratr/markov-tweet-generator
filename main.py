import numpy as np
import random
import re
from collections import defaultdict
from google.cloud import bigquery

client = bigquery.Client()

markov_graph = defaultdict(lambda: defaultdict(lambda: [0, True]))

# test_dict = defaultdict(lambda: defaultdict(lambda: [0, True]))

# test_dict['numbers']['test'] = [1, False]
# test_dict['numbers']['asdf'] = [1, False]
# #print(test_dict['numbers']['dfdfdf'])

# print('keys:')
# print(test_dict['numbers'].keys())

# print('values:')
# print(test_dict['numbers'].values())

# print('testing mapped values:')
# for value in test_dict['numbers'].values():
#     print(value[0])

# print(list(map(lambda x: x[0], test_dict['numbers'].values())))


# sadfsdfsdffsd



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
            next_word_weight = obj["weight"]
            next_word_stop = obj["stop"] # if the word is a "stop word" that leads to a dead end
            markov_graph[current_word][next_word] = [next_word_weight, next_word_stop]


get_markov_graph()

print('graph created')

# TODO:
# Select the start word randomly based what have actually been common start words
# if a start word has not been specified in the function

# For x first words, e.g. 6, don't return any words that don't have a next word in the graph
# (stop words). After x words do some probabilistic determination on when it's a good
# time to stop. E.g. based on how often the word has ended a tweet. The longer the tweet
# is the more likely the word should be to end it.

# graph = the markov graph as a dictionary
# distance = the number of words per tweet
# start_node = the word to start with
def walk_graph(graph, distance=8, start_node=None):
    """Returns a list of words from a randomly weighted walk."""
    if distance <= 0:
        return []

    # If not given, pick a start node at random.
    if not start_node:
        start_node = random.choice(list(graph.keys()))

    # Pick a destination using weighted distribution.
    choices = list(markov_graph[start_node].keys())
    weights = list(map(lambda x: x[0], markov_graph[start_node].values()))
    chosen_word = np.random.choice(choices, None, p=weights)

    # Check if the chosen word leads to a dead end. Only allowed if min length for tweet has been reached.
    if markov_graph[start_node][chosen_word][1] == True:
        print('is a stop word, should not be used ' + chosen_word)
        # the word is skipped
        return walk_graph(
            graph, distance=distance,
            start_node=start_node)

    return [chosen_word] + walk_graph(
        graph, distance=distance-1,
        start_node=chosen_word)

# Print tweets
start_word = 'tämä'
for i in range(10):
    print(start_word + ' ' + ' '.join(walk_graph(
        markov_graph, distance=12, start_node=start_word)), '\n')
