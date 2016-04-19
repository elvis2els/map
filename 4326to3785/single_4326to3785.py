from pyproj import Proj, transform


def epsg4326to3785(x, y):
    epsg4326 = Proj(proj='utm', zone=50, ellps='WGS84')
    epsg3785 = Proj(init='epsg:3785')
    x1, y1 = epsg4326(x, y)
    x2, y2 = transform(epsg4326, epsg3785, x1, y1)
    return x2, y2


def main():
    ofs = open("20121104-1.csv", "w")
    with open("20121104.csv", "r") as ifs:
        while True:
            line = ifs.readline()
            if not line:
                break
            column = line.split(",")
            x, y = epsg4326to3785(column[3], column[4])
            input_str = column[0] + "," + column[1] + \
                "," + column[2] + ",%24.16f,%24.17f" % (x, y) + "\n"
            ofs.write(input_str)


if __name__ == '__main__':
    main()
