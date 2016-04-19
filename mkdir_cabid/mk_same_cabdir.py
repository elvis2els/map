import time
import os
import argparse


def main():
    srcpath = args.path[0]
    despath = args.path[1]
    if not os.path.exists(srcpath):
        raise Exception("srcpath is not exists")
    if not os.path.exists(despath):
        raise Exception("despath is not exists")

    for root, cabids, filenames in os.walk(srcpath):
        for cabid in cabids:
            os.mkdir(despath + "/" + cabid, 0755)

parser = argparse.ArgumentParser(
    description="using to create the same dir from srcpath to despath")

parser.add_argument(
    'path', help="input path and output path", nargs=2)

args = parser.parse_args()

if args.path:
    start = time.time()
    try:
        main()
    except Exception, e:
        print(e)
    end = time.time()
    print(end - start)
