import configparser
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm


class ARIMA(object):

    def __init__(self, path):
        self.path = path
        self.weekday = os.path.splitext(os.path.basename(path))[0]
        df = pd.read_csv(path, index_col='time_group', names=[
            'time_group', 'duration'])
        self.data = self.fillna(df)
        self.conf = os.path.join(os.path.dirname(path), 'default.conf')
        if not os.path.exists(self.conf):
            with open(self.conf, 'w') as f:
                config = configparser.ConfigParser()
                config.read(self.conf)
                config.add_section('weekday')
                config.add_section('weekend')
                config.write(f)

    def getConf(self, df, option):
        config = configparser.ConfigParser()
        config.read(self.conf)
        opts = config.options(self.weekday)
        if option in opts:
            return eval(config.get(self.weekday, option))
        else:
            p, q = self.select_order(df)
            if option in ['diff', 'logdiff']:
                order = (p, 1, q)
            else:
                order = (p, q)
            config.set(self.weekday, option, str(order))
            config.write(open(self.conf, 'w'))
            return order

    def fillna(self, df):
        first_index, last_index = df.index.get_values()[
            0], df.index.get_values()[-1]
        indexlist = [x for x in range(first_index, last_index + 1)]
        df = pd.Series(np.nan, index=indexlist).add(
            df['duration'], fill_value=0).fillna(method='pad')
        return pd.DataFrame(df, index=df.index, columns=['duration'])

    def printADF(self, df):
        t = sm.tsa.stattools.adfuller(np.array(df['duration']))
        output = pd.DataFrame(index=['Test Statistic Value', "p-value", "Lags Used", "Number of Observations Used",
                                     "Critical Value(1%)", "Critical Value(5%)", "Critical Value(10%)"], columns=['value'])
        output['value']['Test Statistic Value'] = t[0]
        output['value']['p-value'] = t[1]
        output['value']['Lags Used'] = t[2]
        output['value']['Number of Observations Used'] = t[3]
        output['value']['Critical Value(1%)'] = t[4]['1%']
        output['value']['Critical Value(5%)'] = t[4]['5%']
        output['value']['Critical Value(10%)'] = t[4]['10%']
        print(output)

    def drawPlot(self, df):
        df.plot(figsize=(12, 8))
        plt.show()

    def analize_acf_pacf(self, df):
        fig = plt.figure(figsize=(12, 8))
        ax1 = fig.add_subplot(211)
        fig = sm.graphics.tsa.plot_acf(df, lags=80, ax=ax1)
        ax2 = fig.add_subplot(212)
        fig = sm.graphics.tsa.plot_pacf(df, lags=80, ax=ax2)
        plt.show()

    def analize_original(self):
        self.normal_analize(self.data)

    def select_order(self, df):
        order = sm.tsa.arma_order_select_ic(
            np.array(df['duration']), max_ar=7, max_ma=7, ic=['aic'])
        return order.aic_min_order

    def restore_diff(self, df, df_proceed):
        predictions_proceed_cumsum = df_proceed.cumsum()

        # print(df_proceed)
        # print(predictions_proceed_cumsum)

        predictions = pd.Series(df['duration'].iloc[
                                0], index=self.data.index)
        predictions = predictions.add(
            predictions_proceed_cumsum, fill_value=0)
        return predictions

    def draw_compare(self, df1, df2):
        plt.figure(figsize=(12, 8))
        plt.plot(df1)
        plt.plot(df2, color='red')
        plt.show()

    def normal_analize(self, df):
        self.drawPlot(df)
        self.analize_acf_pacf(df)
        self.printADF(df)

    def arima_diff(self):
        data_diff = self.data.diff().dropna()
        self.normal_analize(data_diff)

        # print(data_diff)
        # print(data_diff.cumsum())

        order = self.getConf(data_diff, 'diff')
        print('order={}'.format(order))
        model = sm.tsa.ARIMA(np.array(self.data['duration']), order).fit()
        print(model.summary())

        predictions_diff = pd.Series(
            model.fittedvalues, copy=True, index=data_diff.index)
        predictions = self.restore_diff(self.data, predictions_diff)

        self.draw_compare(data_diff, predictions_diff)
        self.draw_compare(self.data, predictions)


    def arima_log(self):
        data_log = np.log(self.data)
        self.normal_analize(data_log)

        order = self.getConf(data_log, 'log')
        print('order={}'.format(order))
        model = sm.tsa.ARMA(np.array(data_log['duration']), order).fit()
        print(model.summary())

        predictions_log = pd.Series(
            model.fittedvalues, copy=True, index=data_log.index)
        predictions = np.exp(predictions_log)

        self.draw_compare(data_log, predictions_log)
        self.draw_compare(self.data, predictions)

    
    def arima_logdiff(self):
        data_log = np.log(self.data)
        data_logdiff = data_log.diff().dropna()
        self.normal_analize(data_logdiff)

        order = self.getConf(data_logdiff, 'logdiff')
        print('order={}'.format(order))
        model = sm.tsa.ARIMA(np.array(data_log['duration']), order).fit()
        print(model.summary())

        predictions_logdiff = pd.Series(
            model.fittedvalues, copy=True, index=data_logdiff.index)
        predictions_log = self.restore_diff(data_log, predictions_logdiff)
        predictions = np.exp(predictions_log)

        self.draw_compare(data_logdiff, predictions_logdiff)
        self.draw_compare(data_log, predictions_log)
        self.draw_compare(self.data, predictions)
