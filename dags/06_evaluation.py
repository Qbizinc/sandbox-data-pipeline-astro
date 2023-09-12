from utils2 import stage_in_gcs, get_vars

def evaluation(expand=None, **kwargs):
    
    from helpers.helpers import apply_model
    import pandas as pd

    vars = get_vars()
    local_path = vars['local_path']
    
    categories_pd = pd.read_csv(f'{local_path}/catageries.csv') \
                      .filter(items = [expand], axis = 0)
                      
    train_name = kwargs['train_name']
    train_pd = pd.read_csv(f'{local_path}/{train_name}.csv') \
                 .merge(categories_pd)

    validation_name = kwargs['val_name']
    val_pd = pd.read_csv(f'{local_path}/{validation_name}.csv') \
                 .merge(categories_pd)
                 
    apply_model(train_pd, 'train', expand,
                model_path = f'{local_path}/model/')
    
    apply_model(val_pd, 'evaluation', expand,
                model_path = f'{local_path}/model/')