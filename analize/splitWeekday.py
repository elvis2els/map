#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import argparse
import datetime

parser = argparse.ArgumentParser(description="show time distribute")
parser.add_argument('input', help="the file of time duration")
parser.add_argument('output', help="only include output file path and file name")
args = parser.parse_args()

def main():
    f = open(args.input, 'r')
    lines = f.readlines()
    outWeekday = []
    outWeekend = []
    for line in lines:
        vecLine = line.strip('\n').split(',')
        date = datetime.datetime.strptime(vecLine[1], '%Y-%m-%d %H:%M:%S.%f')
        if date.weekday() <= 4:
            outWeekday.append(line)
        else:
            outWeekend.append(line)
    f.close()

    outfile = open(args.output+'weekday.csv', 'w')
    outfile.writelines(outWeekday)
    outfile.close()

    outfile = open(args.output+'weekend.csv', 'w')
    outfile.writelines(outWeekend)
    outfile.close()


if __name__ == '__main__':
    main()