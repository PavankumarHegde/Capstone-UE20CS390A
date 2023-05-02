import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob

def sentiment_analysis_vader(text):
    analyzer = SentimentIntensityAnalyzer()
    scores = analyzer.polarity_scores(text)
    sentiment = 'positive' if scores['compound'] > 0 else 'negative' if scores['compound'] < 0 else 'neutral'
    return sentiment

def sentiment_analysis_blob(text):
    blob = TextBlob(text)
    sentiment = 'positive' if blob.sentiment.polarity > 0 else 'negative' if blob.sentiment.polarity < 0 else 'neutral'
    return sentiment

def apply_sentiment_analysis(data, method='vader'):
    if method == 'vader':
        data['sentiment'] = data['text'].apply(sentiment_analysis_vader)
    elif method == 'blob':
        data['sentiment'] = data['text'].apply(sentiment_analysis_blob)
    return data

