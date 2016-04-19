# !/usr/bin/python
import time
import os
import argparse


def main():
    rootpath = args.path[0]
    despath = args.path[1]
    f = open(despath, 'w')
    for root, cabids, filenames in os.walk(rootpath):
        for cabid in cabids:
            cabidpath = rootpath + "/" + cabid
            for cabidroot, dirs, days in os.walk(cabidpath):
                for day in days:
                    filepath = os.path.realpath(cabidroot) + "/" + day + "\n"
                    f.write(filepath)

parser = argparse.ArgumentParser(
    description="using to create input_path from srcspath to desfile path")

parser.add_argument('path', help="input path", nargs=2)

args = parser.parse_args()

if __name__ == "__main__":
    if args.path:
        start = time.time()
        main()
        end = time.time()
        print(end - start)
