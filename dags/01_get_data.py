from utils2 import download_files, upload_files


# @stage_in_gcs(
#     source_files=["product.csv", "lineitem.csv", "order.csv"],
#     output_files=["data.csv"], )
def main(**kwargs):
    import pandas as pd

    input_files = ["product.csv", "lineitem.csv", "order.csv"]
    output_files = ["data.csv"]
    local_path = download_files(files=input_files,
                                prefix_var="source_prefix")

    # Read product catalog
    product_pd = pd.read_csv(f"{local_path}/{input_files[0]}")
    product_pd = product_pd[["p_product_id", "department"]].set_index("p_product_id")

    # Read transactional table lineitem
    lineitem_pd = pd.read_csv(f"{local_path}/{input_files[1]}")
    lineitem_pd = lineitem_pd[["li_order_id", "li_product_id", "quantity"]].set_index(
        "li_product_id"
    )

    # Join lineitem with product to identify the department
    lineitem_pd = lineitem_pd.join(product_pd, how="inner").set_index("li_order_id")

    # Read transactional table order
    order_pd = pd.read_csv(f"{local_path}/{input_files[2]}")
    order_pd = order_pd[["o_order_id", "date", "store"]].set_index("o_order_id")

    # Join order with lineitem to identify the quantity of each order
    data_pd = order_pd.join(lineitem_pd, how="inner").reset_index()
    data_pd["date"] = data_pd["date"].astype("datetime64[ns]")

    # Create a calendar to group orders by week
    dates_range = pd.date_range(min(data_pd["date"]), max(data_pd["date"]), freq="D")
    calendar_pd = pd.DataFrame({"date": dates_range})
    calendar_pd["start_week"] = calendar_pd["date"]
    calendar_pd["year"] = calendar_pd["date"].dt.isocalendar().year
    calendar_pd["week"] = calendar_pd["date"].dt.isocalendar().week
    tmp_pd = (
        calendar_pd.groupby(["year", "week"], as_index=False)
        .agg({"start_week": "min"})
        .set_index(["year", "week"])
    )

    calendar_pd = (
        calendar_pd.drop(["start_week"], axis=1)
        .set_index(["year", "week"])
        .join(tmp_pd, how="inner")
        .reset_index()
        .drop(["year", "week"], axis=1)
        .set_index("date")
    )

    # Join orders with calendar to group transactions by week
    data_pd = (
        data_pd.set_index("date")
        .join(calendar_pd, how="inner")
        .reset_index()
        .drop(["date"], axis=1)
        .rename(columns={"start_week": "date"})
        .groupby(["date", "store", "department"], as_index=False)
        .agg({"quantity": "sum"})
    )

    # save data_pd
    data_pd.to_csv(f"{local_path}/{output_files[0]}", index=False)
    upload_files(files=output_files)
