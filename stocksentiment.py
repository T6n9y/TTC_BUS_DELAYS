import requests
import psycopg2
import pandas as pd
from datetime import datetime
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# Setup
nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()

# API Keys
NEWS_API_KEY = 'df9649db936a4bc0b1dfb9cb8aa5b801'
ALPHA_VANTAGE_KEY = 'M6UBQHO9G1UFA6BJ'

# PostgreSQL Configuration
POSTGRES = {
    'host': 'localhost',
    'database': 'stocksentiment',
    'user': 'postgres',
    'password': 'admin@123'
}

# Stocks to Track
STOCKS = {
    'TSLA': ('Tesla', 'United States'),
    'AAPL': ('Apple', 'United States'),
    'NVDA': ('Nvidia', 'United States'),
    'KO': ('Coca-Cola', 'United States'),
    '005930.KQ': ('Samsung', 'South Korea')
}

# Connect to PostgreSQL
conn = psycopg2.connect(**POSTGRES)
cur = conn.cursor()

# Logging function
def log_api(stock, source, status, message):
    try:
        cur.execute("""
            INSERT INTO api_log (stock_symbol, source, status, message)
            VALUES (%s, %s, %s, %s)
        """, (stock, source, status, message))
    except Exception as e:
        print(f"Log API error: {e}")

# Get latest closing price from Alpha Vantage
def get_latest_close_price(symbol):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_KEY}'
    response = requests.get(url).json()
    series = response["Time Series (Daily)"]
    latest_date = sorted(series.keys(), reverse=True)[0]
    close_price = float(series[latest_date]["4. close"])
    return close_price

# Get 7-day rolling volatility
def get_volatility(symbol):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_KEY}&outputsize=compact'
    response = requests.get(url).json()
    series = response.get("Time Series (Daily)", {})
    if len(series) < 7:
        return None

    df = pd.DataFrame(series).T
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df['close'] = df['4. close'].astype(float)
    df['pct_change'] = df['close'].pct_change()
    df['volatility_7d'] = df['pct_change'].rolling(window=7).std()
    return float(df.iloc[-1]['volatility_7d'])

# Main processing loop
for symbol in STOCKS:
    name = STOCKS[symbol][0]
    country = STOCKS[symbol][1]

    try:
        price = float(get_latest_close_price(symbol))
        volatility = get_volatility(symbol)

        news_url = f'https://newsapi.org/v2/everything?q={name}&language=en&sortBy=publishedAt&pageSize=5&apiKey={NEWS_API_KEY}'
        news_resp = requests.get(news_url)
        articles = news_resp.json().get('articles', [])

        for article in articles:
            headline = str(article['title'])
            published_at = article['publishedAt']
            source_name = str(article['source']['name'])
            sentiment = float(sia.polarity_scores(headline)['compound'])

            cur.execute("""
                INSERT INTO stock_sentiment 
                (stock_symbol, company_name, headline, source, sentiment_score, published_at, price_at_time, country, volatility_7d)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                str(symbol),
                str(name),
                headline,
                source_name,
                sentiment,
                published_at,
                price,
                str(country),
                float(volatility) if volatility is not None else None
            ))

        log_api(symbol, "NewsAPI", "Success", f"{len(articles)} headlines processed.")

    except Exception as e:
        conn.rollback()  # Important to reset transaction
        log_api(symbol, "AlphaVantage", "Failed", str(e))
        continue

# Finalize the transaction
conn.commit()
cur.close()
conn.close()
