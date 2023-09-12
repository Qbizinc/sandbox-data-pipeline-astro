from utils2 import stage_in_gcs, get_vars

def infering_data(expand=None, **kwargs):
    
    from helpers.helpers import apply_model
    import pandas as pd
    
    vars = get_vars()
    local_path = vars['local_path']
    
    categories_pd = pd.read_csv(f'{local_path}/catageries.csv') \
                      .filter(items = [expand], axis = 0)

    data_pd = pd.read_csv(f'{local_path}/clean_data.csv') \
                 .merge(categories_pd)
                 
    apply_model(data_pd, 'infering', expand,
                model_path = f'{local_path}/model/')