import os

from utils2 import upload_files, download_files


def main(expand=None, **kwargs):
    import pandas as pd
    from helpers.helpers import lag_pandas

    input_files = ['data.csv']
    local_path = download_files(files=input_files)

    time_var = 'date'
    data_pd = pd.read_csv(f'{local_path}/{input_files[0]}')

    data_pd['department'] = data_pd['department'].str.replace('/', '_') \
        .str.replace(' ', '_')

    data_grp_state_pd = data_pd.set_index(kwargs['grp_vars'] + [time_var]) \
        .groupby(level=kwargs['grp_vars'] + [time_var])['quantity'] \
        .sum() \
        .reset_index()

    lagged_pd = lag_pandas(data_grp_state_pd, 'date', kwargs['grp_vars'], 12)
    grp_name = '_'.join(kwargs['grp_vars'])
    os.makedirs(f'{local_path}/', exist_ok=True)
    lagged_pd.to_csv(f'{local_path}/lagged_{grp_name}.csv',
                     index=False
                     )
    output_files = [f'lagged_{grp_name}.csv']
    upload_files(files=output_files)
