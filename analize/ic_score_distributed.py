import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

def get_stats(group):
    return {'count': group.count()}

if __name__ == '__main__':
    engine = create_engine("mysql+pymysql://root:123456@192.168.3.199:3306/path_restore")
    query = 'SELECT * FROM visual_cross WHERE type=5'
    df = pd.read_sql_query(query, engine)
    print(df.describe())
    bins = [x for x in range(0,231,10)]
    cats = pd.cut(df['score'], bins)
    grouped = df['score'].groupby(cats)
    bin_counts = grouped.apply(get_stats).unstack()
    bin_counts = bin_counts[bin_counts['count'] > 10]
    bin_counts.plot(kind='bar', alpha=0.5, rot=0)
    plt.show()
