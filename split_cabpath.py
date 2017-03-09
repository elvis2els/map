import os
import time
import argparse
# import pdb
# pdb.set_trace()

parser = argparse.ArgumentParser(
    description="using to split the every cab path to a single file")
parser.add_argument(
    'path', help="source path and output path. The path must be exists", nargs=2)
args = parser.parse_args()


def main():
    srcpath = args.path[0]
    despath = args.path[1]
    if not os.path.exists(srcpath):
        raise Exception("srcpath is not exists")
    if not os.path.exists(despath):
        raise Exception("despath is not exists")

    for srcroot, srcdirs, srcfiles in os.walk(srcpath):
        for cabid in srcdirs:
            cabid_path = srcpath + '/' + cabid
            for cabidroot, cabidirs, cabidfiles in os.walk(cabid_path):
                for cabidfile in cabidfiles:
                    day = cabidfile[:-4]
                    write_cabid_path = despath + '/' + cabid
                    ifs = open(cabid_path + '/' + cabidfile, 'r')
                    all_lines = ifs.readlines()
                    ifs.close()
                    i = 0
                    is_a_line = False
                    for line in all_lines:
                        if line[0] == '1':
                            if is_a_line:
                                ofs = open(
                                    write_cabid_path + '/' + day + '-' + str(i) + ".csv", 'w')
                                ofs.writelines(lines)
                                ofs.close()
                                i += 1
                            is_a_line = True
                            lines = []
                            lines.append(line)
                        if is_a_line and line[0] == '4':
                            lines.append(line)


# def test():
#     path = "20121102.csv"
#     day = path[:-4]
#     ifs = open(path, 'r')
#     all_lines = ifs.readlines()
#     i = 0
#     is_a_line = False
#     for line in all_lines:
#         if line[0] == '1':
#             if is_a_line:
#                 ofs = open(day + '-' + str(i) + ".csv", 'w')
#                 ofs.writelines(lines)
#                 i += 1
#             is_a_line = True
#             lines = []
#             lines.append(line)
#         if is_a_line and line[0] == '4':
#             lines.append(line)


if __name__ == '__main__':
    if args.path:
        start = time.time()
        # test()
        main()
        print(time.time() - start)
