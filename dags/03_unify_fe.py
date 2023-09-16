from utils2 import download_files, upload_files


def main(expand=None, **kwargs):
    import pandas as pd

    input_files = []
    for grp_name in ['store', 'department', 'store_department']:
        input_files.append(f"lagged_{grp_name}.csv")

    output_files = ["clean_data.csv"]
    local_path = download_files(files=input_files)

    data_list = []
    for input_file in input_files:
        tmp_pd = pd.read_csv(f'{local_path}/{input_file}')
        print(tmp_pd.columns)
        data_list.append(tmp_pd)

    data_pd = pd.merge(data_list[2],
                       data_list[0].drop(['quantity'], axis=1),
                       on=['date', 'store'])
    data_pd = pd.merge(data_pd,
                       data_list[1].drop(['quantity'], axis=1),
                       on=['date', 'department']) \
        .reset_index(drop=True)

    data_pd.to_csv(f'{local_path}/{output_files[0]}', index=False)
    upload_files(files=output_files)
