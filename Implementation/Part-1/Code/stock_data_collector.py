import yfinance as yf
import requests
import json
from datetime import datetime
from kafka import KafkaProducer


def get_stock_data(symbol):
    # Get data from Yahoo Finance API
    yahoo_fin_url = "https://finance.yahoo.com/quote/" + symbol + "/history?p=" + symbol
    res = requests.get(yahoo_fin_url)
    if res.status_code == 200:
        stock_data = res.text
    else:
        stock_data = None

    # Get data from Polygon API
    polygon_url = "https://api.polygon.io/v2/aggs/ticker/" + symbol + "/prev?unadjusted=true&apiKey=YOUR_API_KEY"
    res = requests.get(polygon_url)
    if res.status_code == 200:
        prev_close = json.loads(res.text)["results"][0]["c"]
    else:
        prev_close = None

    # Get data from Yahoo Finance using yfinance package
    start_date = datetime.now().strftime('%Y-%m-%d')
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    stock = yf.Ticker(symbol)
    stock_history = stock.history(start=start_date, end=end_date)

    # Combine data
    stock_data_dict = {"symbol": symbol,
                       "stock_data": stock_data,
                       "prev_close": prev_close,
                       "stock_history": stock_history.to_dict()}
    return stock_data_dict


def collect_stock_data(symbols, kafka_server):
    producer = KafkaProducer(bootstrap_servers=[kafka_server], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    for symbol in symbols:
        stock_data = get_stock_data(symbol)
        producer.send('stock', value=stock_data)
        print(f"{symbol} data sent to Kafka")


if __name__ == '__main__':
    symbols = ["AAPL", "TSLA", "GOOG"]
    kafka_server = "localhost:9092"
    collect_stock_data(symbols, kafka_server)

