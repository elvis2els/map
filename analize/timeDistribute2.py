#!/usr/local/bin/python3
# -*- coding: UTF-8 -*-
import argparse
import datetime as dt
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


def main():
    df = pd.read_csv(args.file)

    # 列类型转换
    df['time_x'], df['time_y'], df['duration'] = \
        pd.to_datetime(df['time_x']).dt.time, \
        pd.to_datetime(df['time_y']).dt.time, \
        pd.to_timedelta(df['duration'])

    # df['time_x'], df['time_y'] = 
    df['duration'] = df['duration'] / np.timedelta64(1, 's')
    print(df.head())
    print(df.dtypes)

    plt.plot(df['time_x'], df['duration'], 'ob')
    plt.show()

    # print(df['duration'] / np.timedelta64(1, 's'))

    # f = open(args.file, 'r')
    # lines = f.readlines()
    # for line in lines:
    #     line = line.strip('\n').split(',')
    #     datalimit = 0
    #     date = datetime.datetime.strptime(line[1], '%Y-%m-%d %H:%M:%S.%f')
    #     time = date.time()
    #     time = datetime.timedelta(hours=time.hour, minutes=time.minute, seconds=time.second,
    #                               milliseconds=time.microsecond / 1000).total_seconds() / 3600
    #     if args.mode == 'time':
    #         timeduration = datetime.datetime.strptime(line[2], '%H:%M:%S.%f')
    #         x, y = time, datetime.timedelta(hours=timeduration.hour, minutes=timeduration.minute,
    #                                         seconds=timeduration.second, milliseconds=timeduration.microsecond / 1000).total_seconds()
    #         datalimit = 1000
    #     else:
    #         x, y = time, float(line[3])
    #         datalimit = 150
    #     if y <= datalimit:
    #         if args.week:
    #             if args.week == 'weekday':
    #                 if date.weekday() <= 4:
    #                     plt.scatter(x, y)
    #             elif args.week == 'weekend':
    #                 if date.weekday() >= 5:
    #                     plt.scatter(x, y)
    #         else:
    #             if date.weekday() <= 4:
    #                 plt.scatter(x, y, c="red")
    #             else:
    #                 plt.scatter(x, y, c="blue")
    # f.close()

    # if args.mode == 'time':
    #     plt.title('Time Distribute')
    #     plt.xlabel('hour')
    #     plt.ylabel('second')
    # else:
    #     plt.title('Speed Distribute')
    #     plt.xlabel('hour')
    #     plt.ylabel('km/h')
    # plt.legend()
    # plt.grid(True)
    # plt.show()

if __name__ == '__main__':
    main()
