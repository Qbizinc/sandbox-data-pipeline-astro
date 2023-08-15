import time
import pandas as pd
from pathlib import Path

def main(**kwargs):
    data_pd = pd.read_csv('include/data/data-demo.csv')
    for col in ['family', 'state', 'city']:
        data_pd[col] = data_pd[col].str.replace('/', '_') \
                                    .str.replace(' ', '_')

    # Path(kwargs['data_path']).mkdir(parents=True, exist_ok=True)
    # data_pd.to_csv('{}/original_data.csv'.format(kwargs['data_path']),
    #                index = False)
    print("get_data")