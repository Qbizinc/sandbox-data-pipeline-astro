from utils2 import stage_in_gcs, get_vars

def main(expand=None, **kwargs):
    
    import pandas as pd
    from sklearn.model_selection import train_test_split
    
    vars = get_vars()
    local_path = vars['local_path']

    dataset_name = kwargs['dataset']
    data_pd = pd.read_csv(f'{local_path}/{dataset_name}.csv')

    target_column = kwargs['target']
    X = data_pd.drop([target_column], axis = 1)
    y = data_pd[target_column]
   
    X_train, X_val, y_train, y_val = train_test_split(X, y,
                                                      test_size = kwargs['test_size'])
    
    train_pd = pd.DataFrame(X_train, columns = X_train.columns) \
                 .reset_index(drop = True)
    train_pd[target_column] = y_train
    
    val_pd = pd.DataFrame(X_val, columns = X_val.columns) \
                 .reset_index(drop = True)

    val_pd[target_column] = y_val
    
    train_name = kwargs['train_name']
    val_name = kwargs['val_name']
    train_pd.to_csv(f'{local_path}/{train_name}.csv', index = False)
    val_pd.to_csv(f'{local_path}/{val_name}.csv', index = False)
    
    categories_pd = train_pd[['store', 'department']] \
        .drop_duplicates() \
        .sort_values(by = ['store', 'department']) \
        .reset_index(drop = True)
        
    categories_pd.to_csv(f'{local_path}/catageries.csv', index = False)
    print('Total categories: ', len(categories_pd.index.tolist()))

    # TODO: Save into GCP
