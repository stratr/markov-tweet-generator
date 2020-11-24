import numpy as np
import random
import pickle
from collections import defaultdict
from google.cloud import storage
from google.cloud import bigquery


storage_client = storage.Client()
bucket = storage_client.bucket('markov_generator')
folder = 'all_meps'

def load_pickle(bucket, folder, file_name):
    # Load a pickled file and return a dictionary

    file_path = '{}/{}.pickle'.format(folder, file_name)
    print('Loading file: {}'.format(file_path))
    blob = bucket.blob(file_path)
    
    return pickle.loads(blob.download_as_string())

# Load the markov graph
markov_graph = load_pickle(bucket, folder, 'markov_graph')
# print(markov_graph['ja']['suru'][1])
# asdfdfsdfs

# Load start words
start_words = load_pickle(bucket, folder, 'start_words')


def generate_tweet(graph, min_distance=5, max_distance=8, start_word=None):
    """
    Generates a new tweet using the markov graph that was defined earlier.
    The length of the tweet is something between the min_distance and max_distance.
    The function attempts to stop the tweet with some word that makes some sense.
    start_word is the first word the the generator uses to start iterating through the markov graph. If not specified a random wrod will be used.
    """

    # Tweets have an increasing probability of ending after the min number of words has been achieved
    num_end_tries = max_distance - min_distance
    end_try = 1 

    if not start_word:
        start_word = random.choice(list(graph.keys()))

    tweet_words = [start_word]
    current_word = start_word

    i = max_distance - 1 # the start word has already been added
    while i > 0:
        # Pick the next word based on the weights and current word
        choices = list(markov_graph[current_word].keys())
        weights = list(map(lambda x: x[0], markov_graph[current_word].values()))
        next_word = np.random.choice(choices, None, p=weights)

        # Check if the chosen word is a stop word (dead end)
        if markov_graph[current_word][next_word][1] == True:
            if len(tweet_words) >= min_distance:
                # End the tweet with the chosen word
                print('Ending the word because of a stop word')
                tweet_words.append(next_word)
                break
            else:
                # Try to pick another word
                continue

        # If min tweet length has been already reached take a flip to see if the chosen word will be the last one
        if len(tweet_words) >= min_distance:
            end_probability = markov_graph[current_word][next_word][2] * end_try / num_end_tries
            end_try += 1
            if np.random.random_sample() <= end_probability:
                print('end the tweet with end probability: ' + str(end_probability))
                tweet_words.append(next_word)
                break

        tweet_words.append(next_word)
        current_word = next_word
        i -= 1
    
    return ' '.join(tweet_words).capitalize()


# #TODO: transform this function into a loop (iterative) insteadof the recursive format. That way it shouldn't run into recursion limits.
# def walk_graph(graph, min_distance=5, max_distance=8, start_node=None, end_tries=0):
#     """Returns a list of words from a randomly weighted walk."""
#     if max_distance <= 0:
#         return []

#     # If not given, pick a start node at random.
#     if not start_node:
#         start_node = random.choice(list(graph.keys()))

#     # Pick a destination using weighted distribution.
#     choices = list(markov_graph[start_node].keys())
#     weights = list(map(lambda x: x[0], markov_graph[start_node].values()))
#     chosen_word = np.random.choice(choices, None, p=weights)

#     # Check if the chosen word leads to a dead end. Only allowed if min length for tweet has been reached.
#     if markov_graph[start_node][chosen_word][1] == True:
#         # allow the tweet to end if the word is a "stop word" and min_distance has been reached
#         if min_distance <= 0:
#             #print('end the text in a stop word: ' + chosen_word)
#             return [chosen_word]
#         # skip the chosen word and retry
#         else:
#             #print('chosen word is a dead end, choose another one: ' + chosen_word)
#             return walk_graph(
#                 graph, min_distance=min_distance, max_distance=max_distance,
#                 start_node=start_node, end_tries=end_tries)

#     # If min_distance is reached use an increasing probabibility and the word probability as a possibility to end the tweet before
#     if min_distance <= 0:
#         end_probability = markov_graph[start_node][chosen_word][2] * ((min_distance-1)*-1/end_tries)
#         if np.random.random_sample() <= end_probability:
#             #print('end the tweet with end probability: ' + str(end_probability))
#             return [chosen_word]

#     return [chosen_word] + walk_graph(
#         graph, min_distance=min_distance-1, max_distance=max_distance-1,
#         start_node=chosen_word, end_tries=end_tries)


def generateTweets(graph, min_distance=6, max_distance=16, start_node=None, number_of_tweets=10):
    generated_tweets = []
    num_end_tries = max_distance - min_distance

    start_nodes = []
    if not start_node:
        # use random start words based on actual weights
        start_nodes = [np.random.choice(start_words, None, start_weights) for x in range(number_of_tweets)]
    else:
        # use the selected start word
        start_nodes = [start_node for x in range(number_of_tweets)]

    for start_word in start_nodes:
        generated_tweets.append({u'text': start_word.capitalize() + ' ' + ' '.join(walk_graph(
            graph, min_distance=min_distance, max_distance=max_distance, start_node=start_word, end_tries=num_end_tries))})
    
    return generated_tweets


def random_start_word(start_words):
    choices = list(start_words.keys())
    weights = list(start_words.values())
    start_word = np.random.choice(choices, None, p=weights)

    return start_word

print(random_start_word(start_words))
sdfdsfsdfsfdfsd


# load the markov graph
new_tweet = generate_tweet(markov_graph, min_distance=5, max_distance=16, start_word="ja")
print(new_tweet)

sadfasfdfsda

rows_to_insert = generateTweets(markov_graph, min_distance=6, max_distance=16, start_node=None, number_of_tweets=1)

#print(rows_to_insert)

# Insert the generated tweets into bigquery
#rows_to_insert = map(lambda x: {u"text": x}, tweets)
table_id = 'tanelis.markov_chain.generated_tweets'

errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
if errors == []:
    print("New rows have been added.")
else:
    print("Encountered errors while inserting rows: {}".format(errors))