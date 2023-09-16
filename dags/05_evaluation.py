from utils2 import download_files


def evaluation(expand=None, **kwargs):
    from helpers.helpers import apply_model
    import pandas as pd

    train_name = kwargs['train_name']
    validation_name = kwargs['val_name']

    input_files = ["categories.csv", f"{train_name}.csv", f"{validation_name}.csv"]
    output_files = ["categories.csv"]
    local_path = download_files(files=input_files, overwrite=False)

    categories_pd = pd.read_csv(f'{local_path}/categories.csv') \
        .filter(items=[expand], axis=0)

    train_pd = pd.read_csv(f'{local_path}/{train_name}.csv') \
        .merge(categories_pd)

    val_pd = pd.read_csv(f'{local_path}/{validation_name}.csv') \
        .merge(categories_pd)

    apply_model(train_pd, 'train', expand,
                model_path=f'{local_path}/model/')

    apply_model(val_pd, 'evaluation', expand,
                model_path=f'{local_path}/model/')
