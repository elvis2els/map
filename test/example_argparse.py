# -*- coding:utf-8 -*-

import argparse

parser = argparse.ArgumentParser(description='argparse tester')
parser.add_argument(
        "-v", "--verbose", help="increase output verbosity", action="store_true")
parser.add_argument(
        "numbers", type=int, help="numbers to calculate", nargs='+')
parser.add_argument(
        '-s', '--sum', help="sum all numbers", action='store_true', default=True)

args = parser.parse_args()

print("Input: ", args.numbers)
resluts = args.numbers

if args.verbose:
    print("hello world")

if args.sum:
    resluts = sum(args.numbers)
    print("Sum: %s" % resluts)
