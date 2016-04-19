import tarfile
import os
import time
import shutil
from pyproj import Proj, transform


def decompressiontar(sourcepath, despath):
    tar = tarfile.open(sourcepath)
    names = tar.getnames()
    for name in names:
        tar.extract(name, path=despath)
    tar.close()


def buildlist(sourcepath_name):
    with open(sourcepath_name, "r") as f:
        line = f.readline()
        columns = []
        while True:
            line = f.readline()
            if not line:
                f.close()
                return columns
            columns.append(line.split(','))


def epsg4326to3785(x, y):
    epsg4326 = Proj(proj='utm', zone=50, ellps='WGS84')
    epsg3785 = Proj(init='epsg:3785')
    x1, y1 = epsg4326(x, y)
    x2, y2 = transform(epsg4326, epsg3785, x1, y1)
    return x2, y2


def main():
    rootpath = "D:\\map\\2012.11 北京出租车GPS数据"
    ysrootpath = rootpath + "\\原始数据"
    # 先把原始数据解压
    for root, dirs, filenames in os.walk(ysrootpath):
        for day in dirs:  # 递归天数
            tarpath = ysrootpath + "\\" + day
            untarpath = tarpath + "\\untar"
            for root_day, dirs_day, filenames_day in os.walk(tarpath):
                for filename in filenames_day:
                    tarpath_file = tarpath + "\\" + filename
                    decompressiontar(tarpath_file, untarpath)
            despath = rootpath + "\\epsg3785\\" + day
            for root_day, dirs_day, filenames_day in os.walk(untarpath):
                if not os.path.exists(despath):
                    os.makedirs(despath)
                for filename in filenames_day:
                    sourcepath_file = untarpath + "\\" + filename
                    columns = buildlist(sourcepath_file)
                    despath_file = despath + "\\" + filename
                    f = open(despath_file, 'a')
                    for column in columns:
                        try:
                            x, y = epsg4326to3785(column[4], column[5])
                            input_str = column[0] + "," + column[1] + "," + column[2] + "," + column[
                                3] + ",%24.16f,%24.17f," % (x, y) + column[6] + "," + column[7] + "," + column[8]
                            f.write(input_str)
                        except RuntimeError as e:
                            pass
                    f.close()
            shutil.rmtree(untarpath)
            print("sucess:" + day)


if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print(end - start)
