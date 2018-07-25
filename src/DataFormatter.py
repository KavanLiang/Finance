import pandas as pd
import pickle
import os
from time import sleep

TICKER_URL = "https://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download"
ALPHA_VANTAGE_KEY = ''


def save_nasdaq_tickers() -> pd.core.frame.DataFrame:
    tickers = pd.read_csv(TICKER_URL)
    tickers = tickers[['Symbol', 'Name']]
    with open("nasdaq_tickers", "wb") as f:
        pickle.dump(tickers, f)
    return tickers


def alpha_vantage_query_daily(ticker: str) -> str:
    return f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY' \
           f'&symbol={ticker}&apikey={ALPHA_VANTAGE_KEY}&outputsize=full&datatype=csv'


def get_alpha_vantage_data(reload_tickers=False):
    if reload_tickers:
        tickers = save_nasdaq_tickers()
    else:
        with open("nasdaq_tickers", "rb") as f:
            tickers = pickle.load(f)
    if not os.path.exists('stock_data'):
        os.makedirs('stock_data')
    for index, row in tickers.iterrows():
        ticker = row['Symbol']
        if not os.path.exists(f'stock_data/{ticker}.csv'):
            sleep(12)
            print(f'Processing {ticker}.csv. Currently at file {index} of {tickers.shape[0]}.')
            df = pd.read_csv(alpha_vantage_query_daily(ticker))
            df.to_csv(f'stock_data/{ticker}.csv')
        else:
            print(f'Already have {ticker}.')

def check_tickers():
    with open("nasdaq_tickers", "rb") as f:
        tickers = pickle.load(f)
    print(tickers)


if __name__ == '__main__':
    get_alpha_vantage_data()
