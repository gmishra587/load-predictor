import pandas as pd

def daily_features(df: pd.DataFrame):
    df["shortage"] = df["demand_scheduled_mw"] - df["actual_load_mw"]
    df["date"] = df["start_time"].dt.date

    daily = df.groupby("date").agg(
        energy_consumption_mu=("actual_load_mw", lambda x: x.sum()/1000),
        average_load_mw=("actual_load_mw", "mean"),
        peak_load_mw=("actual_load_mw", "max"),
        peak_shortage_mw=("shortage", "max"),
        demand_max=("demand_scheduled_mw", "max"),
        energy_shortage_mu=("shortage", lambda x: x.sum()/1000)
    ).reset_index()

    daily["peak_shortage_percent"] = (
        daily["peak_shortage_mw"] / daily["demand_max"] * 100
    )

    daily["energy_shortage_percent"] = (
        daily["energy_shortage_mu"] /
        (daily["energy_consumption_mu"] + daily["energy_shortage_mu"]) * 100
    )

    return daily
