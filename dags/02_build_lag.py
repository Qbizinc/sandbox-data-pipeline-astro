from utils2 import stage_in_gcs, get_vars

def main(expand=None, **kwargs):
    
    import pandas as pd
    from helpers.helpers import lag_pandas
    import os
    
    vars = get_vars()
    local_path = vars['local_path']

    time_var = 'date'
    data_pd = pd.read_csv(f'{local_path}/data.csv')
    
    data_pd['department'] = data_pd['department'].str.replace('/', '_') \
                                       .str.replace(' ', '_')
    
    data_grp_state_pd = data_pd.set_index(kwargs['grp_vars'] + [time_var]) \
                           .groupby(level = kwargs['grp_vars'] + [time_var])['quantity'] \
                           .sum() \
                           .reset_index()

    lagged_pd = lag_pandas(data_grp_state_pd, 'date', kwargs['grp_vars'], 12)
    grp_name = '_'.join(kwargs['grp_vars'])
    os.makedirs(f'{local_path}/tmp/', exist_ok = True)
    lagged_pd.to_csv(f'{local_path}/tmp/lagged_{grp_name}.csv',
                        index = False
                    )
    # TODO: Save in GCP