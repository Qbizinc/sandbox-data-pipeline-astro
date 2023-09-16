from utils2 import download_files


def infering_data(expand=None, **kwargs):
    from helpers.helpers import apply_model
    import pandas as pd

    input_files = [f"clean_data.csv", "categories.csv"]
    local_path = download_files(files=input_files)

    categories_pd = pd.read_csv(f'{local_path}/categories.csv') \
        .filter(items=[expand], axis=0)

    data_pd = pd.read_csv(f'{local_path}/clean_data.csv') \
        .merge(categories_pd)

    apply_model(data_pd, 'infering', expand,
                model_path=f'{local_path}/model/')
