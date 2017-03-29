#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import argparse
import datetime as dt
import math
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

parser = argparse.ArgumentParser(description="show time distribute")
parser.add_argument('file', help="the file of time duration")
# parser.add_argument('--week', choices=['weekday', 'weekend'],
#                     required=True, help="analize weekday or weekend")
# parser.add_argument('-c', '--clusFile', default=None,
#                     help='the path of vecluster')
args = parser.parse_args()


def draw_weekdays_plot(df):
    plt.plot(df['time_x'].dt.time, df['duration'], 'ob')
    plt.show()


def is_outlier(row, state):
    q1 = state.loc[row.time_group]['q1']
    q3 = state.loc[row.time_group]['q3']
    iqr = q3 - q1
    if row.duration > (q3 + 1.5 * iqr) or row.duration < (q1 - 1.5 * iqr):
        return True
    else:
        return False


def main():
    df = pd.read_csv(args.file)

    # 列类型转换
    df['time_x'], df['time_y'], df['duration'] = \
        pd.to_datetime(df['time_x']), \
        pd.to_datetime(df['time_y']), \
        pd.to_timedelta(df['duration'])

    df['duration'] = df['duration'] / np.timedelta64(1, 's')
    df['time_group'] = ((df['time_x'].dt.hour * 60 +
                         df['time_x'].dt.minute) / 5).apply(math.ceil)

    print("原始长度： {}".format(len(df)))
    grouped = df.groupby('time_group')
    statBefore = pd.DataFrame({'q1': grouped['duration'].quantile(.25),
                               'q3': grouped['duration'].quantile(.75)})
    df['outlier'] = df.apply(is_outlier, axis=1, args=(statBefore,))
    df = df[~(df.outlier)]
    del df['outlier']
    print("filtered长度： {}".format(len(df)))

    df_weekday = df[df['time_x'].dt.weekday < 6]
    df_weekend = df[df['time_x'].dt.weekday >= 6]
    draw_weekdays_plot(df_weekday)
    draw_weekdays_plot(df_weekend)

    df_weekday.boxplot(column='duration', by='time_group')
    plt.show()
    df_weekend.boxplot(column='duration', by='time_group')
    plt.show()

if __name__ == '__main__':
    main()
