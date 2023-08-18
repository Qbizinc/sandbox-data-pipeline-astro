from include.helpers import *


def main(expand=None, **kwargs):
    data_pd = pd.read_csv("include/data/data-demo.csv")
    for col in ["family", "state", "city"]:
        data_pd[col] = data_pd[col].str.replace("/", "_").str.replace(" ", "_")

    data_grp_pd = (
        data_pd.set_index(kwargs["grp_vars"] + ["quarter"])
        .groupby(level=kwargs["grp_vars"] + ["quarter"])["sales"]
        .sum()
        .reset_index()
    )

    lagged_pd = lag_pandas(data_grp_pd, kwargs["grp_vars"], 4)

    categories_dict = lagged_pd[kwargs["grp_vars"]].drop_duplicates().to_dict("split")
    index_list = [item for item in range(len(categories_dict["data"]))]
    keywords_list = ["-".join(item) for item in categories_dict["data"]]
    models_dict = {
        item: xgb.XGBRegressor(n_estimators=10, objective="reg:squarederror")
        for item in keywords_list
    }

    scores_list = []
    # TODO: instead of loop have the expand
    for item in index_list:
        score = train_model(
            item, lagged_pd, kwargs["grp_vars"], models_dict, categories_dict
        )
        scores_list.append(score)

    # print(kwargs['task_id'] + " is executed.")
