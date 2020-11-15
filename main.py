import numpy as np
import random
import re
from collections import defaultdict
from google.cloud import bigquery

client = bigquery.Client()

# initiate markov graph dictionary
markov_graph = defaultdict(lambda: defaultdict(lambda: [0, True]))

# start word weights
start_words = []
start_weights = []

# build the ditionary and get start word weights
def get_source_data():
    # The source data is already prepared in BigQuery. Read it into dictionary format.
    query_sql = "SELECT * FROM `tanelis.markov_chain.markov_graph`"

    client = bigquery.Client()
    query_job = client.query(query_sql)
    results = query_job.result()

    for row in results:
        current_word = row["current_word"]
        start_weight = row["start_probability"]
        next_array = row["next"]

        # Get the next words data for the markov graph
        for obj in next_array:
            next_word = obj["word"]
            next_word_weight = obj["weight"]
            next_word_stop = obj["stop"] # if the word is a "stop word" that leads to a dead end
            next_word_end_probability = obj["end_probability"] # a probability for each word on how often they should end the tweet once min words are reached

            markov_graph[current_word][next_word] = [
                next_word_weight, next_word_stop, next_word_end_probability]

        # Get the start word weights for when the tweet is generated with a random start word
        start_words.append(current_word)
        start_weights.append(start_weight)

get_source_data()

def walk_graph(graph, min_distance=5, max_distance=8, start_node=None, end_tries=0):
    """Returns a list of words from a randomly weighted walk."""
    if max_distance <= 0:
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
        # allow the tweet to end if the word is a "stop word" and min_distance has been reached
        if min_distance <= 0:
            #print('end the text in a stop word: ' + chosen_word)
            return [chosen_word]
        # skip the chosen word and retry
        else:
            #print('chosen word is a dead end, choose another one: ' + chosen_word)
            return walk_graph(
                graph, min_distance=min_distance, max_distance=max_distance,
                start_node=start_node, end_tries=end_tries)

    # If min_distance is reached use an increasing probabibility and the word probability as a possibility to end the tweet before
    if min_distance <= 0:
        end_probability = markov_graph[start_node][chosen_word][2] * ((min_distance-1)*-1/end_tries)
        if np.random.random_sample() <= end_probability:
            #print('end the tweet with end probability: ' + str(end_probability))
            return [chosen_word]

    return [chosen_word] + walk_graph(
        graph, min_distance=min_distance-1, max_distance=max_distance-1,
        start_node=chosen_word, end_tries=end_tries)


def generateTweets(graph, min_distance=6, max_distance=16, start_node=None, number_of_tweets=10):
    num_end_tries = max_distance - min_distance

    start_nodes = []
    if not start_node:
        # use random start words based on actual weights
        start_nodes = [np.random.choice(start_words, None, start_weights) for x in range(number_of_tweets)]
    else:
        # use the selected start word
        start_nodes = [start_node for x in range(number_of_tweets)]

    for start_word in start_nodes:
        print(start_word.capitalize() + ' ' + ' '.join(walk_graph(
            graph, min_distance=min_distance, max_distance=max_distance, start_node=start_word, end_tries=num_end_tries)), '\n')


generateTweets(markov_graph, min_distance=6, max_distance=16, start_node=None, number_of_tweets=30)