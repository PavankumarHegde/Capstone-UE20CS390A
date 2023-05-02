import tweepy
import json
from kafka import KafkaProducer


def collect_social_media_data(api_key, api_secret_key, access_token, access_token_secret, kafka_bootstrap_servers, kafka_topic):
    # Set up Twitter API authentication
    auth = tweepy.OAuthHandler(api_key, api_secret_key)
    auth.set_access_token(access_token, access_token_secret)

    # Create API object
    api = tweepy.API(auth)

    # Create Kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))

    # Set up keywords to track
    keywords = ['Bitcoin', 'Ethereum', 'Cryptocurrency', 'Blockchain']

    # Set up a stream listener
    class TwitterStreamListener(tweepy.StreamListener):
        def on_status(self, status):
            try:
                tweet = {}
                tweet['text'] = status.text
                tweet['created_at'] = str(status.created_at)
                tweet['user'] = status.user.screen_name
                producer.send(kafka_topic, value=tweet)
            except Exception as e:
                print(e)

    # Create stream object
    stream_listener = TwitterStreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)

    # Start streaming
    stream.filter(track=keywords)

