import pandas as pd

def build_lags_state():
    df = pd.read_csv("include/data/demo_data.csv")
    for state in ["Tungurahua", "Esmeraldas", "Pichincha"]:
        for lag in range(1, 13):
            df[f"cases_lag_{lag:02}"] = df[f"cases"].shift(lag)
        df.to_csv(f"output/{state}/demo_data_lags.csv", index=False)

