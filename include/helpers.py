import joblib
import pandas as pd
from sklearn.model_selection import cross_val_score


def lag_pandas(df, grp, n_lags):
    data_grp_pd = df.set_index(grp)

    lag_dict = {f'sales-lag{lag}': data_grp_pd.drop(['quarter'], axis = 1) \
                                    .groupby(level = grp) \
                                    .shift(lag) \
                                    .rename(columns=lambda x: x+f'lag{lag}') for lag in range(1,n_lags)}

    lagged_pd = data_grp_pd.assign(**lag_dict) \
                    .reset_index() \
                    .dropna()
                    
    return lagged_pd

def train_model(item, df, grp, model_dict, categories_dict,
                model_path = 'include/models'):
    
    value_list = categories_dict['data'][item]
    tmp_dict = {f'{col}': f'{val}' \
                for val, col in zip(value_list,
                                    categories_dict['columns'])}
    
    tmp_pd = pd.DataFrame(tmp_dict, index = [0])
    train_pd = df.merge(tmp_pd)
    
    X = train_pd.drop(['sales', 'quarter', *grp], axis = 1)
    y = train_pd['sales']
    
    model = model_dict['-'.join(value_list)]
    scores = cross_val_score(model, X, y, cv = 5)
    model.fit(X, y)
    
    #Path(model_path).mkdir(parents=True, exist_ok=True)
    #joblib.dump(model, '{}/{}.joblib'.format(model_path, '-'.join(value_list)), compress = 1)
    
    return scores

def apply_model(item, df, grp, categories_dict,
                model_path = 'include/models'):
    
    value_list = categories_dict['data'][item]
    state_category = '-'.join(value_list[1:])
    tmp_dict = {f'{col}': f'{val}' \
                for val, col in zip(value_list,
                                    categories_dict['columns'])}
    
    tmp_pd = pd.DataFrame(tmp_dict, index = [0])
    train_pd = df.merge(tmp_pd)
    
    X = train_pd.drop(['sales', 'quarter', *grp], axis = 1)
    
    model = joblib.load(model, '{}/{}.joblib'.format(model_path, '-'.join(value_list)),
         compress = 1)
    yhat = model.predict(X)
    inference_pd['yhat'] = yhat
    inference_pd = inference_pd[grp + ['quarter', 'yhat']]
    
    return inference_pd


def save_data(X, y, file_name):
    
    data_pd = pd.DataFrame(X, columns = X.columns) \
                 .reset_index()
    data_pd['y'] = y
    
    data_pd.to_csv(file_name,index = False)
    
    return None