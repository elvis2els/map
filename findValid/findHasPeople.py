import time
import argparse
import os

def main():
    id = '2'
    day = '20151030'
    inputpath = '/dataPool/map/2015cabGPS_splitpath/' + id + '/' + day + '.txt'
    outpath = '/dataPool/map/hasPeople/' + id
    if not os.path.exists(outpath):
        os.mkdir(outpath)
    ifs = open(inputpath, 'r')
    lines = ifs.readlines()
    ifs.close()

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
            ofs = open(outpath + '/' + day + '-' + str(i) + '.txt', 'w')
            ofs.writelines(hasline)
            hasline = []
            i += 1
            pre = False
            

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print(end - start)
