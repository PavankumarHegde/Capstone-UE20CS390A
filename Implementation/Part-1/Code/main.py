from flask import Flask, render_template, request
from social_media_data_collector import collect_social_media_data
from stock_data_collector import collect_stock_data
from news_data_collector import collect_news_data
from sentiment_analysis import perform_sentiment_analysis
from data_processing import combine_data_by_date, convert_language_to_english, filter_data_by_date_range
from machine_learning import perform_machine_learning
from batch_processing import store_data_in_mysql
from real_time_graph import plot_graph

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/collect_social_media_data')
def collect_social_media_data_button():
    collect_social_media_data()
    return 'Social media data collection process initiated!'

@app.route('/collect_stock_data')
def collect_stock_data_button():
    collect_stock_data()
    return 'Stock data collection process initiated!'

@app.route('/collect_news_data')
def collect_news_data_button():
    collect_news_data()
    return 'News data collection process initiated!'

@app.route('/perform_sentiment_analysis')
def perform_sentiment_analysis_button():
    perform_sentiment_analysis()
    return 'Sentiment analysis process initiated!'

@app.route('/combine_data_by_date')
def combine_data_by_date_button():
    combine_data_by_date()
    return 'Data combination process initiated!'

@app.route('/convert_language_to_english')
def convert_language_to_english_button():
    convert_language_to_english()
    return 'Language conversion process initiated!'

@app.route('/filter_data_by_date_range')
def filter_data_by_date_range_button():
    filter_data_by_date_range()
    return 'Data filtering process initiated!'

@app.route('/perform_machine_learning')
def perform_machine_learning_button():
    perform_machine_learning()
    return 'Machine learning process initiated!'

@app.route('/store_data_in_mysql')
def store_data_in_mysql_button():
    store_data_in_mysql()
    return 'Batch processing and data storage process initiated!'

@app.route('/plot_graph')
def plot_graph_button():
    plot_graph()
    return 'Real-time graph plotting process initiated!'

if __name__ == '__main__':
    app.run(debug=True)

