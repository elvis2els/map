#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import argparse
import datetime as dt
import math
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

parser = argparse.ArgumentParser(description="filter outlier data")
parser.add_argument('file', help="the file of time duration")
args = parser.parse_args()


def is_outlier(row, state):
    q1 = state.loc[row.time_group]['q1']
    q3 = state.loc[row.time_group]['q3']
    iqr = q3 - q1
    if row.duration > (q3 + 1.5 * iqr) or row.duration < (q1 - 1.5 * iqr):
        return True
    else:
        return False


def mean_to_csv(df, path):
    grouped = df['duration'].groupby(df['time_group'])
    grouped.mean().to_csv(path)

def main():
    df = pd.read_csv(args.file, parse_dates=['time_x', 'time_y'])

    # 列类型转换
    df['duration'] = pd.to_timedelta(df['duration'])

    df['duration'] = df['duration'] / np.timedelta64(1, 's')
    df['time_group'] = ((df['time_x'].dt.hour * 60 +
                         df['time_x'].dt.minute) / 15).apply(math.ceil)

    print("原始长度： {}".format(len(df)))
    grouped = df.groupby('time_group')
    statBefore = pd.DataFrame({'q1': grouped['duration'].quantile(.25),
                               'q3': grouped['duration'].quantile(.75)})
    df['outlier'] = df.apply(is_outlier, axis=1, args=(statBefore,))
    df = df[~(df.outlier)]
    df['weekday'] = df['time_x'].dt.weekday
    del df['outlier']
    print("filtered长度： {}".format(len(df)))

    dirname = os.path.dirname(args.file)
    filename = '{}-filered.csv'.format(os.path.basename(args.file)[:-4])
    outPath = os.path.join(dirname, filename)
    df.to_csv(outPath, index=False)

    df_weekday = df[df['weekday'] < 6]
    df_weekend = df[df['weekday'] >= 6]
    df_weekday.to_csv(os.path.join(dirname, 'weekday.csv'), index=False)
    df_weekend.to_csv(os.path.join(dirname, 'weekend.csv'), index=False)
    # mean_to_csv(df_weekday, os.path.join(dirname, 'weekday.csv'))
    # mean_to_csv(df_weekend, os.path.join(dirname, 'weekend.csv'))



if __name__ == '__main__':
    main()
