import os
import argparse

parser = argparse.ArgumentParser(description="using to make findValid input and ouput path file")
parser.add_argument('path', help="input source path and ouput destination path", nargs=2)
args = parser.parse_args()


def main():
    srcpath = args.path[0]
    despath = args.path[1]
    inoutputPath = []
    for root, days, filenames in os.walk(srcpath):
        for day in days:
            daypath = srcpath + day + '/'
            for root2, dirnames, filenames2 in os.walk(daypath):
                for file in filenames2:
                    inoutputPath.append(daypath + file + '\n')
                    inoutputPath.append("/dataPool/map/2015valid2/" + day + '/' + file + '\n')
    f = open(despath, 'w')
    f.writelines(inoutputPath)
    f.close()


if __name__ == '__main__':
    if args.path:
        try:
            main()
        except Exception as e:
            print(e)
