import numpy as np
import random
import re
from collections import defaultdict
from google.cloud import bigquery

client = bigquery.Client()

markov_graph = defaultdict(lambda: defaultdict(lambda: [0, True]))

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
            # if the word is a "stop word" that leads to a dead end
            next_word_stop = obj["stop"]
            # a probability has been counted for how often the next word has ended a tweet
            next_word_last_word_probability = obj["last_word_probability"]
            markov_graph[current_word][next_word] = [
                next_word_weight, next_word_stop, next_word_last_word_probability]


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
# max_distance = the number of words per tweet
# start_node = the word to start with


def walk_graph(graph, min_distance=5, max_distance=8, start_node=None, end_tries=None):
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
            print('end the text in a stop word: ' + chosen_word)
            return [chosen_word]
        # skip the chosen word and retry
        else:
            print('chosen word is a dead end, choose another one: ' + chosen_word)
            return walk_graph(
                graph, min_distance=min_distance, max_distance=max_distance,
                start_node=start_node)

    # If min_distance is reached use an increasing probabibility and the word probability as a possibility to end the tweet before
    if min_distance <= 0:
        # TODO: add some check if the end_tries was provided
        print(end_tries) # this is now being passed as None always for some reason, some bug
        print('min distance reached')


    return [chosen_word] + walk_graph(
        graph, min_distance=min_distance-1, max_distance=max_distance-1,
        start_node=chosen_word)


# Print tweets
start_word = '@MarinSanna'


def generateTweets(graph, min_distance=6, max_distance=16, start_node=None, number_of_tweets=10):
    for i in range(number_of_tweets):
        print(start_word + ' ' + ' '.join(walk_graph(
            graph, min_distance=min_distance, max_distance=max_distance, start_node=start_node, end_tries=(max_distance-min_distance))), '\n')

generateTweets(markov_graph, min_distance=6, max_distance=16, start_node=start_word, number_of_tweets=10)