from .helpers import lag_pandas


def main(expand=None, **kwargs):
    import pandas as pd

    time_var = "quarter"
    data_pd = pd.read_csv("dags/data/data-demo.csv")
    for col in ["family", "state", "city"]:
        data_pd[col] = data_pd[col].str.replace("/", "_").str.replace(" ", "_")

    data_grp_state_pd = (
        data_pd.set_index(kwargs["grp_vars"] + [time_var])
        .groupby(level=kwargs["grp_vars"] + [time_var])["sales"]
        .sum()
        .reset_index()
    )

    # lagged_pd = lag_pandas(data_grp_state_pd, kwargs['grp_vars'], 4)
    # lagged_pd.to_csv('dags/data/tmp/lagged_{}.csv' \
    #                     .format('-'.join(kwargs['grp_vars'])),
    #                     index = False
    #                 )
    # print(kwargs['task_id'] + " is executed.")
