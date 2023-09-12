from utils2 import stage_in_gcs, get_vars

def main(expand=None, **kwargs):
    
    import pandas as pd
    
    vars = get_vars()
    local_path = vars['local_path']

    data_list = []
    for grp_name in ['store', 'department', 'store_department']:
        tmp_pd = pd.read_csv(f'{local_path}/tmp/lagged_{grp_name}.csv')
        print(tmp_pd.columns)
        data_list.append(tmp_pd)

    data_pd = pd.merge(data_list[2],
                       data_list[0].drop(['quantity'], axis = 1),
                       on = ['date', 'store'])
    data_pd = pd.merge(data_pd,
                       data_list[1].drop(['quantity'], axis = 1),
                       on = ['date', 'department']) \
                .reset_index(drop = True)
    
    data_pd.to_csv(f'{local_path}/clean_data.csv', index = False)
    # TODO: Save into GCP
