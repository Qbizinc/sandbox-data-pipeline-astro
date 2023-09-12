import pandas as pd
import lightgbm as lgb
import xgboost as xgb
from sklearn.model_selection import cross_val_score
from pathlib import Path
import joblib

def lag_pandas(df, time_var, grp_var, n_lags):
    data_grp_var_pd = df.set_index(grp_var)

    col_name = '_'.join(grp_var)
    lag_dict = {f'{col_name}_lag{lag}': data_grp_var_pd.drop([time_var], axis = 1) \
                                    .groupby(level = grp_var) \
                                    .shift(lag) \
                                    .rename(columns=lambda x: x+f'lag{lag}') for lag in range(1,n_lags)}

    lagged_pd = data_grp_var_pd.assign(**lag_dict) \
                    .reset_index() \
                    .dropna()
                    
    return lagged_pd

def train_model(train_pd, model, model_name, category,
                model_path = '/models'):
    
    X = train_pd.drop(['quantity', 'date', 'store', 'department'], axis = 1)
    y = train_pd['quantity']
    if X.shape[0] == 0:
        return None

    scores = None
    scores = cross_val_score(model, X, y, cv = 5)
    model.fit(X, y)
    
    Path(f'{model_path}/{model_name}/').mkdir(parents=True, exist_ok=True)
    joblib.dump(model, f'{model_path}/{model_name}/{category}.joblib', compress = 1)
    
    return scores

def apply_model(df, type_evaluation, model_name,
                model_path = '/models'):
    

    if 'quantity' in df.columns:
        X = df.drop(['quantity', 'date'], axis = 1)
        y = df['quantity']
    else:
        X = df.drop(['date'], axis = 1)
    
    prediction_dict = dict()
    for model_name in ['xgb', 'lgb']:
        try:
            model = joblib.load(f'{model_path}/{model_name}/{category}.joblib',
                                compress = 1)
        except:
            return None

        prediction_dict[model_name] = model.predict(X)
        
    df['yhat'] = 0.5 * (prediction_dict['xgb'] + prediction_dict['lgb'])
    
    if 'quantity' in df.columns:
        df['quantity'] = y
    
    df.to_csv(f'{local_path}/{type_evaluation}/category.csv', index = False)
