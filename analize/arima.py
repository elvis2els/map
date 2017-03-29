import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm


class ARIMA(object):

    def __init__(self, path):
        self.path = path
        self.data = pd.read_csv(path, index_col='time_group', names=[
                                'time_group', 'duration'])

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

        p, q = self.select_order(data_diff)
        order = (p, 1, q)
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

        order = self.select_order(data_log)
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

        p, q = self.select_order(data_logdiff)
        order = (p,1,q)
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

        
