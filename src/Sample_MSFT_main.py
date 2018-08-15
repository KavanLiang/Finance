"""
A sample program that predicts MSFT stock data using linear regression and MatPlotLib
"""

import DataFormatter
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from math import ceil
from sklearn import linear_model, preprocessing, model_selection
import datetime

AV_key = "" #AVKey here
DataFormatter.set_av_key(AV_key)

if __name__ == "__main__":
    df = DataFormatter.get_alpha_vantage_data('MSFT', AV_key)
    df.index = pd.to_datetime(df.index)
    projection_out = int(ceil(0.05 * len(df)))
    df['label'] = df['adjusted_close'].shift(projection_out)
    df.dropna(inplace=True)
    X = preprocessing.scale(np.array(df.drop(['label'], 1)))
    y = np.array(df['label'])

    X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, test_size=0.2)
    clf = linear_model.LinearRegression(n_jobs=-1)
    clf.fit(X_train, y_train)
    print(clf.score(X_test, y_test))

    projection_set = clf.predict(X[:projection_out])[::-1]
    df['projected'] = np.nan

    curr_date = df.iloc[0].name.to_pydatetime()
    for i in projection_set:
        curr_date += datetime.timedelta(days=1)
        df.loc[curr_date] = [np.nan for _ in range(len(df.columns)-1)]+[i]

    df['adjusted_close'].plot()
    df['projected'].plot()
    plt.legend(loc=2)
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.show()
