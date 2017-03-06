#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import argparse
import datetime
import os
import re

import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description="show time distribute")
parser.add_argument('file', help="the file of time duration")
parser.add_argument('--week', choices=['weekday', 'weekend'],
                    required=True, help="analize weekday or weekend")
parser.add_argument(
    '--mode', choices=['time', 'speed'], required=True, help="use time or speed")
parser.add_argument('-c', '--clusFile', default=None,
                    help='the path of vecluster')
args = parser.parse_args()


def main():
    f = open(args.file, 'r')
    lines = f.readlines()
    for line in lines:
        line = line.strip('\n').split(',')
        datalimit = 0
        date = datetime.datetime.strptime(line[1], '%Y-%m-%d %H:%M:%S.%f')
        time = date.time()
        time = datetime.timedelta(hours=time.hour, minutes=time.minute, seconds=time.second,
                                  milliseconds=time.microsecond / 1000).total_seconds() / 3600
        if args.mode == 'time':
            timeduration = datetime.datetime.strptime(line[2], '%H:%M:%S.%f')
            x, y = time, datetime.timedelta(hours=timeduration.hour, minutes=timeduration.minute,
                                            seconds=timeduration.second, milliseconds=timeduration.microsecond / 1000).total_seconds()
            datalimit = 1000
        else:
            x, y = time, float(line[3])
            datalimit = 150
        if y <= datalimit:
            if args.week:
                if args.week == 'weekday':
                    if date.weekday() <= 4:
                        plt.scatter(x, y)
                elif args.week == 'weekend':
                    if date.weekday() >= 5:
                        plt.scatter(x, y)
            else:
                if date.weekday() <= 4:
                    plt.scatter(x, y, c="red")
                else:
                    plt.scatter(x, y, c="blue")
    f.close()

    if args.clusFile:
        f = open(os.path.expanduser(args.clusFile), 'r')
        lines = f.readlines()
        for line in lines:
            line = line.strip('\n').split('-')
            time_split = [int(r) for r in re.split(':|\.', line[0])]
            if len(time_split) != 4:
                time_split.append(0)
            clus_time_start = datetime.timedelta(hours=time_split[0], minutes=time_split[1],
                                                 seconds=time_split[2], milliseconds=time_split[3] / 1000).total_seconds() / 3600
            time_split = [int(r) for r in re.split(':|\.', line[1])]
            if len(time_split) != 4:
                time_split.append(0)
            clus_time_end = datetime.timedelta(hours=time_split[0], minutes=time_split[1],
                                                 seconds=time_split[2], milliseconds=time_split[3] / 1000).total_seconds() / 3600
            plt.plot([clus_time_start, clus_time_end], [line[2], line[2]], c='red')

    if args.mode == 'time':
        plt.title('Time Distribute')
        plt.xlabel('hour')
        plt.ylabel('second')
    else:
        plt.title('Speed Distribute')
        plt.xlabel('hour')
        plt.ylabel('km/h')
    plt.legend()
    plt.grid(True)
    plt.show()

if __name__ == '__main__':
    main()
