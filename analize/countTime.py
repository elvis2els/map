import time

path = '/home/elvis/map/analizeTime/20151030.txt'
outputPath = '/home/elvis/map/countTime/'


def analizeTime(lines):
    count = {}
    sum = 0
    for line in lines:
        column = line.split(",")
        key = int(float(column[0]) / 10)
        sum += int(column[1].strip())
        if key not in count:
            count[key] = int(column[1].strip())
        else:
            count[key] += int(column[1].strip())
    print(count)
    print("sum: " + str(sum))


def main():
    ifs = open(path, 'r')
    lines = ifs.readlines()
    ifs.close()
    analizeTime(lines)


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
