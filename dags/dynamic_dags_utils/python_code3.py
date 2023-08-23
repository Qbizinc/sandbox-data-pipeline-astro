import pandas as pd
from sklearn.model_selection import train_test_split
from .helpers import save_data


def main(expand=None, **kwargs):
    data_pd = pd.read_csv('dags/data/data-demo.csv')
    for col in ['family', 'state', 'city']:
        data_pd[col] = data_pd[col].str.replace('/', '_') \
                                    .str.replace(' ', '_')
    
    X = data_pd.drop(['sales'], axis = 1)
    y = data_pd['sales']
    
    X_train, y_train, X_val, y_val = train_test_split(X, y,
                                                      test_size = kwargs['test_size'])
    
    # save_data(X_train, y_train, 'dags/data/tmp/train_lagged_{}.csv' \
    #                     .format('-'.join(kwargs['grp_vars'])))
    
    # save_data(X_val, y_val, 'dags/data/tmp/val_lagged_{}.csv' \
    #                     .format('-'.join(kwargs['grp_vars'])))
    
    #print(kwargs['task_id'] + " is executed.")