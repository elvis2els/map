# !/usr/bin/python
import time
import os
import argparse


def main():
    rootpath = args.path[0]
    out_rootpath = args.path[1]
    f = open("output_path", 'w')
    for root, cabids, filenames in os.walk(rootpath):
        for cabid in cabids:
            cabidpath = rootpath + "/" + cabid
            for cabidroot, dirs, days in os.walk(cabidpath):
                for day in days:
                    filepath = os.path.realpath(
                        out_rootpath) + "/" + cabid + "/" + day + "\n"
                    f.write(filepath)

parser = argparse.ArgumentParser(
    description="using to create output_path from srcpath to desfile path")

parser.add_argument('path', help="output path", nargs=2)

args = parser.parse_args()

if __name__ == "__main__":
    if args.path:
        start = time.time()
        main()
        end = time.time()
        print(end - start)
