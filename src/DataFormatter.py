"""
A tool for formatting stock data.
"""
import pandas as pd
import os
from time import sleep

TICKER_URL = "https://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download"
ALPHA_VANTAGE_KEY_1 = ''  # AVKEYHERE
TEMP_BATCH_PATH = 'batch_stock_data'
NASDAQ_TICKERS = 'nasdaq_tickers.csv'


def save_nasdaq_tickers() -> pd.DataFrame:
    tickers = pd.read_csv(TICKER_URL)
    tickers = tickers[['Symbol', 'Name']]
    return tickers


def read_tickers() -> pd.DataFrame:
    """
    :return: a DataFrame containing tickers and names of stocks
    """
    df = pd.read_csv(NASDAQ_TICKERS, dtype={"Symbol": str, "Name": str})[['Symbol', 'Name']]
    return df


def alpha_vantage_query_daily(ticker: str, key: str) -> str:
    return f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED' \
           f'&symbol={ticker}&apikey={key}&outputsize=full&datatype=csv'


def get_alpha_vantage_data(reload_tickers: bool = False) -> None:
    """
    Pulls Ticker data from the AlphaVantage API using tickers fetched from NASDAQ
    :param reload_tickers: whether or not to reload the tickers
    :return:
    """
    if reload_tickers:
        tickers = save_nasdaq_tickers()
    else:
        tickers = read_tickers()
    if not os.path.exists('stock_data'):
        os.makedirs('stock_data')
    for index, row in tickers.iterrows():
        ticker = row['Symbol'].strip()
        if not os.path.exists(f'stock_data/daily_adjusted_{ticker}.csv'):
            print('Waiting...')
            sleep(18)  # avoid throttling without premium key
            try:
                print(f'Processing {ticker}.csv. Currently at file {index} of {tickers.shape[0]}.')
                get_alpha_vantage_data(ticker, ALPHA_VANTAGE_KEY_1)
            except KeyError:
                print(f'AlphaVantage does not have data for {ticker}.')


def set_av_key(key: str):
    ALPHA_VANTAGE_KEY_1 = key


def get_alpha_vantage_data(ticker: str, key: str) -> pd.DataFrame:
    """
    Save a csv file containing data from the passed ticker.
    Ticker will be of format daily_adjusted_{ticker}.csv
    :param ticker: the ticker being saved.
    """
    print(f'Querying: {alpha_vantage_query_daily(ticker, key)}')
    df = pd.read_csv(alpha_vantage_query_daily(ticker, key))
    df.set_index('timestamp', inplace=True)
    df.to_csv(f'stock_data/daily_adjusted_{ticker}.csv')
    print(f'Saved daily_adjusted_{ticker}')
    return df


def compile_data() -> pd.DataFrame:
    """
    :return:  a DataFrame with all the stock data written into it
    """
    joined_data = pd.DataFrame
    files = os.listdir(TEMP_BATCH_PATH)
    for count, filename in enumerate(files):
        if count % 10 == 0:
            print(f'Processing {filename}, file {count + 1} of {len(files)}')
        df = pd.read_csv(f'{TEMP_BATCH_PATH}/{filename}', index_col=0, parse_dates=[0], infer_datetime_format=True)
        df['hl_percent_change'] = (df['high'] - df['low']) / df['low']
        df['oc_percent_change'] = (df['open'] - df['close']) / df['open']
        df.drop(['open', 'high', 'low', 'close', 'volume', 'dividend_amount', 'split_coefficient'], 1, inplace=True)
        curr_ticker = os.path.splitext(filename)[0].split('_')[2]
        df = df.add_suffix(f'_{curr_ticker}')
        if joined_data.empty:
            joined_data = df
        else:
            joined_data = joined_data.join(df, how='outer')
        if count > 300:  # for testing purposes
            break
    joined_data.to_csv("joined_data.csv")
    return joined_data


if __name__ == '__main__':
    pass
