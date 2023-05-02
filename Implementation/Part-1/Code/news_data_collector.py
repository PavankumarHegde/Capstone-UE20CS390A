import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta

# Set up Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Set up API keys
NEWS_API_KEY = 'your_news_api_key_here'
BING_NEWS_API_KEY = 'your_bing_news_api_key_here'
CONTEXTUAL_WEB_API_KEY = 'your_contextual_web_api_key_here'
NEWSCATCHER_API_KEY = 'your_newscatcher_api_key_here'

# Set up URLs
NEWS_API_URL = f'https://newsapi.org/v2/top-headlines?country=us&category=business&apiKey={NEWS_API_KEY}'
BING_NEWS_API_URL = 'https://api.cognitive.microsoft.com/bing/v7.0/news/search'
CONTEXTUAL_WEB_API_URL = 'https://api.contextualweb.io/api/v1/search/news'
NEWSCATCHER_API_URL = 'https://api.newscatcherapi.com/v2/search'

# Set up time range for news search
end_time = datetime.utcnow()
start_time = end_time - timedelta(days=1)

# Convert datetime objects to ISO format for API requests
start_time = start_time.isoformat(timespec='seconds') + 'Z'
end_time = end_time.isoformat(timespec='seconds') + 'Z'

# Define function for sending messages to Kafka topic
def send_to_kafka(topic, message):
    producer.send(topic, json.dumps(message).encode('utf-8'))

# Collect news data from News API
response = requests.get(NEWS_API_URL)
news_data = response.json()['articles']
for article in news_data:
    # Format news data
    news_message = {
        'source': 'News API',
        'title': article['title'],
        'description': article['description'],
        'url': article['url'],
        'publishedAt': article['publishedAt']
    }
    # Send news data to Kafka
    send_to_kafka('news', news_message)
    time.sleep(1) # Wait for 1 second to avoid rate limiting

# Collect news data from Bing News API
params = {
    'q': 'business',
    'mkt': 'en-US',
    'count': 100,
    'freshness': 'Day',
    'sortBy': 'Date',
    'since': start_time,
    'to': end_time
}
headers = {
    'Ocp-Apim-Subscription-Key': BING_NEWS_API_KEY
}
response = requests.get(BING_NEWS_API_URL, params=params, headers=headers)
news_data = response.json()['value']
for article in news_data:
    # Format news data
    news_message = {
        'source': 'Bing News API',
        'title': article['name'],
        'description': article['description'],
        'url': article['url'],
        'publishedAt': article['datePublished']
    }
    # Send news data to Kafka
    send_to_kafka('news', news_message)
    time.sleep(1) # Wait for 1 second to avoid rate limiting

# Collect news data from ContextualWeb News API
params = {
    'q': 'business',
    'lang': 'en',
    'published_at.start': start_time,
    'published_at.end': end_time,
    'api_key': CONTEXTUAL_WEB_API_KEY
}
response = requests.get(CONTEXTUAL_WEB_API_URL, params=params)
news_data = response.json()['data']
for article in news_data:
    # Format news data
    news_message = {
        'source': 'ContextualWeb News API',
        'title': article['title'],
        'description': article['description'],
        'url': article['url'],
        'publishedAt': article['publishedAt'],
        'content': article['content'],
        'category': article['category']
    }

    # Append the news message to the list of news messages
    news_messages.append(news_message)

return news_messages


def collect_news_data():
"""
Collects real-time news data from various news APIs
"""
    # Initialize list to store news messages
    news_messages = []
    # Define news sources and corresponding API keys
    news_sources = {
        'newsapi': 'YOUR_NEWSAPI_KEY_HERE',
        'ContextualWeb News API': 'YOUR_CONTEXTUALWEB_NEWS_API_KEY_HERE',
        'NewsCatcher API': 'YOUR_NEWSCATCHER_API_KEY_HERE',
        'Bing News': 'YOUR_BING_NEWS_API_KEY_HERE'
    }

    # Loop through news sources
    for source, api_key in news_sources.items():
        # Collect news data from the API
        source_news = collect_news_from_api(source, api_key)

        # Append the news data to the list of news messages
        news_messages.extend(source_news)

# Return the list of news messages
return news_messages

