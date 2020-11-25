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
                #print('Ending the word because of a stop word')
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
                #print('end the tweet with end probability: ' + str(end_probability))
                tweet_words.append(next_word)
                break

        tweet_words.append(next_word)
        current_word = next_word
        i -= 1
    
    return ' '.join(tweet_words).capitalize()


def random_start_words(start_words, word_count):
    choices = list(start_words.keys())
    # weights = list(start_words.values())
    weights = np.array(
        list(start_words.values()),
        dtype=np.float64)
    weights /= weights.sum()

    words = [np.random.choice(choices, None, p=weights) for word in range(word_count)]
    start_word = np.random.choice(choices, None, p=weights)

    return words


 # Load the markov graph
markov_graph = load_pickle(bucket, folder, 'markov_graph')

# Load start words
start_words = load_pickle(bucket, folder, 'start_words') 

# Generate x number of start words based on start weights
starts = random_start_words(start_words, 100)

# Generate new tweets from the random start words
tweets = []
for start in starts:
    new_tweet = generate_tweet(markov_graph, min_distance=5, max_distance=16, start_word=start)
    tweets.append(new_tweet)

#print(tweets)


# Insert the generated tweets into bigquery
rows_to_insert = map(lambda x: {u"text": x}, tweets)
table_id = 'tanelis.markov_chain.generated_tweets'
client = bigquery.Client()

errors = client.insert_rows_json(table_id, rows_to_insert)
if errors == []:
    print("New rows have been added.")
else:
    print("Encountered errors while inserting rows: {}".format(errors))