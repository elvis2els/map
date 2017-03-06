import time
import argparse
import os


def findHasLine(filepath, outpath):
    ifs = open(filepath, 'r')
    lines = ifs.readlines()
    ifs.close()

    day = filepath[-12:-4]
    hasline = []
    i = 1
    pre = False
    for line in lines:
        if not pre and line[-2:-1] == '1':
            hasline.append(line)
            pre = True
        elif line[-2:-1] == '1':
            hasline.append(line)
        elif pre and line[-2:-1] == '0':
            if len(hasline) <= 5:
                continue
            if not os.path.exists(outpath):
                os.mkdir(outpath)
            if not os.path.exists(os.path.join(outpath, day)):
                os.mkdir(os.path.join(outpath, day))
            ofs = open(os.path.join(outpath, day, str(i) + '.txt'), 'w')
            ofs.writelines(hasline)
            ofs.close()
            hasline = []
            i += 1
            pre = False


def main():
    rootpath = '/dataPool/map/2015cabGPS_splitpath/'
    outpath = '/dataPool/map/hasPeople/'
    ids = os.listdir(rootpath)
    for id in ids:
        idpath = os.path.join(rootpath, id)
        filenames = os.listdir(idpath)
        for filename in filenames:
            findHasLine(os.path.join(idpath, filename),
                        os.path.join(outpath, id))

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print(end - start)
