#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import argparse
import datetime
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description="show time distribute")
parser.add_argument('file', help="the file of time duration")
parser.add_argument('--week', choices=['weekday', 'weekend'], help="analize weekday or weekend")
parser.add_argument('--mode', choices=['time', 'speed'], help="use time or speed")
args = parser.parse_args()

def main():
    f = open(args.file, 'r')
    lines = f.readlines()
    for line in lines:
        line = line.strip('\n').split(',')
        datalimit = 0
        date = datetime.datetime.strptime(line[1], '%Y-%m-%d %H:%M:%S.%f')
        time = date.time()
        time = datetime.timedelta(hours=time.hour, minutes=time.minute, seconds=time.second, milliseconds=time.microsecond/1000).total_seconds() / 3600
        if args.mode == 'time':
            x, y = time, float(line[2])
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