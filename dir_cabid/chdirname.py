#!/usr/bin/python3
#-*-coding:UTF-8-*-
import argparse
import os
import time

parser = argparse.ArgumentParser(
    description="using to change cabid dirname to human read")
parser.add_argument('path', help="src dir path", nargs=1)
args = parser.parse_args()


def main():
    path = args.path[0]
    os.chdir(path)
    dirnames = os.listdir(path)
    i = 1
    for dirname in dirnames:
        os.rename(dirname, str(i))
        i += 1

if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
