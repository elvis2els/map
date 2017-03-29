import argparse
from arima import ARIMA


parser = argparse.ArgumentParser(description="使用arima拟合")
parser.add_argument('path', help="分析文件路径")
parser.add_argument('--method', choices=['log', 'diff', 'logdiff'],
                    required=True, help="实用分析方法,log or diff or logdiff")
args = parser.parse_args()


def main():
    arima_mod = ARIMA(args.path)
    arima_mod.analize_original()

    if args.method == 'log':
        arima_mod.arima_log()
    elif args.method == 'diff':
        arima_mod.arima_diff()
    else:
        arima_mod.arima_logdiff()

if __name__ == '__main__':
    main()