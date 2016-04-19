import time
import argparse
from pyproj import Proj, transform


def epsg4326to3785(x, y):
    epsg4326 = Proj(proj='utm', zone=50, ellps='WGS84')
    epsg3785 = Proj(init='epsg:3785')
    x1, y1 = epsg4326(x, y)
    x2, y2 = transform(epsg4326, epsg3785, x1, y1)
    return x2, y2


def buildlist(sourcepath_name, outpath):
    with open(sourcepath_name, "r") as f:
        line = f.readline()
        columns = []
        i = 1
        while True:
            if not line:
                f.close()
                break
            column = []
            column = line.split('|')
            if column[2] != '0' and column[3] != '0':
                try:
                    t = time.localtime(int(column[0], 16))
                    t = time.strftime("%Y-%m-%d %H:%M:%S", t)
                    x, y = epsg4326to3785(
                        int(column[3], 16) / 100000.0, int(column[2], 16) / 100000.0)
                except Exception, e:
                    print 'error: ', outpath, 'line: ', i
                    line = f.readline()
                    continue
                line = t + ',' + str('%.16f' % x) + ',' + str('%.16f' % y) + ',' + \
                    column[5] + ',' + column[6] + ',' + column[8] + ',' + column[10] + '\n'
                columns.append(line)
                i += 1
            line = f.readline()
        ofs = open(outpath, 'w')
        ofs.writelines(columns)
        ofs.close()
        print 'success: ', outpath


def main():
    inputpath = args.path[0]
    ifs = open(inputpath, 'r')
    while(True):
        line = ifs.readline()
        if not line:
            break
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
