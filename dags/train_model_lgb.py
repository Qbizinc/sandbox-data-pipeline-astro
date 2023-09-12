from utils2 import stage_in_gcs, get_vars

def train_model_lgb(expand=None, **kwargs):
    
    import pandas as pd
    from helpers.helpers import train_model, lag_pandas
    import xgboost as xgb
    import lightgbm as lgb
    
    vars = get_vars()
    local_path = vars['local_path']
    
    categories_pd = pd.read_csv(f'{local_path}/catageries.csv') \
                      .filter(items = [expand], axis = 0)
                      
    train_name = kwargs['train_name']
    train_pd = pd.read_csv(f'{local_path}/{train_name}.csv') \
                 .merge(categories_pd) \
                 .fillna(0.0)

    if kwargs['model'] == 'xgb':
        model = xgb.XGBRegressor(n_estimators = 50,
                                 objective='reg:squarederror')
    elif kwargs['model'] == 'lgb':
        model = lgb.LGBMRegressor(n_estimators = 50)
    else:
        pass
    
    print(' ===== ')
    print('Expand:', expand)
    print('n_rows:', train_pd.shape)
    print(' ===== ')
    # TODO: instead of loop have the expand
    score = train_model(train_pd, model, kwargs['model'], expand,
                        model_path = f'{local_path}/model')
    print(score)