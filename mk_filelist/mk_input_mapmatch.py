import os
import argparse

parser = argparse.ArgumentParser(description="using to make mapmach input and ouput path file")
parser.add_argument('path', help="input source path and ouput destination path", nargs=2)
args = parser.parse_args()


def main():
    srcpath = args.path[0]
    despath = args.path[1]
    if not os.path.exists(srcpath):
        raise Exception("srcpath is not exists")
    inoutputPath = []
    for srcroot, srcdirs, srcfiles in os.walk(srcpath):
        for cabid in srcdirs:
            cabidpath = srcpath + cabid
            for cabidroot, cabiddir, cabidfiles in os.walk(cabidpath):
                for cabidfile in cabidfiles:
                    inoutputPath.append(cabidpath + '/' + cabidfile + '\n')
                    inoutputPath.append("/map/cabGPS_mapmatch/"+ cabid + "/" + cabidfile
                            + '\n')
    f = open(despath, 'w')
    f.writelines(inoutputPath)
    f.close()


if __name__ == '__main__':
    if args.path:
        try:
            main()
        except Exception as e:
            print(e)
