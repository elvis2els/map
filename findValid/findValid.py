import time
import argparse


def buildlist(sourcepath_name, outpath):
    with open(sourcepath_name, "r") as f:
        line = f.readline()
        while True:
            columns = []
            count = 2000000
            while(count):
                if not line:
                    f.close()
                    return
                column = []
                column = line.split('|')
                if column[0][0] == '5' and column[7] != '40000000':
                    columns.append(line)
                line = f.readline()
                count = count - 1
            ofs = open(outpath, 'a')
            ofs.writelines(columns)
            ofs.close()


def main():
    inputpath = args.path[0]
    ifs = open(inputpath, 'r')
    line = ifs.readline()
    line = line.strip('\n')
    outpath = ifs.readline()
    outpath = outpath.strip('\n')
    buildlist(line, outpath)
    ifs.close()


parser = argparse.ArgumentParser(
    description="using to find valid data")

parser.add_argument('path', help="input path and output path, both dir", nargs=1)

args = parser.parse_args()

if __name__ == '__main__':
    if args.path:
        start = time.time()
        main()
        end = time.time()
        print(end - start)
