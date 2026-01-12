import pandas as pd


def disaggregate(state_forecast: pd.DataFrame, region_forecast: pd.DataFrame, freq: str = "H"):
    out = []

    # -----------------------------
    # Prepare dates
    # -----------------------------
    region_forecast["date"] = region_forecast["ds"].dt.date
    state_forecast["date"] = state_forecast["ds"].dt.date

    for d in state_forecast["date"].unique():
        r = region_forecast[region_forecast["date"] == d].copy()
        if r.empty:
            continue

        # -----------------------------
        # Fraction (shape only)
        # -----------------------------
        total_region = r["yhat"].sum()
        if total_region == 0:
            r["fraction"] = 1 / len(r)
        else:
            r["fraction"] = r["yhat"] / total_region

        # -----------------------------
        # Energy → Average MW
        # -----------------------------
        energy_mu = float(
            state_forecast[state_forecast["date"] == d]["yhat"].values[0]
        )

        average_load_mw = (energy_mu * 1000) / 24

        # -----------------------------
        # ✅ CORRECT HOURLY MW
        # -----------------------------
        r["mw"] = r["fraction"] * average_load_mw * 24

        for _, row in r.iterrows():
            out.append({
                "datetime": row["ds"],
                "mw": round(float(row["mw"]), 2),
                "temperature": round(float(row["temperature_c"]), 1)
                if "temperature_c" in row else None
            })

    return out








# -----------------------------
# Daily stats from hourly/15-min data
# -----------------------------
def daily_stats(hourly_data: list):
    if not hourly_data:
        return {"average_load_mw": 0, "peak_load_mw": 0}

    loads = [h["mw"] for h in hourly_data]
    return {
        "average_load_mw": round(sum(loads) / len(loads), 2),
        "peak_load_mw": round(max(loads), 2)
    }




# -----------------------------
# Resample hourly to 15-min
# -----------------------------
def disaggregate_hourly_to_15min(hourly_df):
    hourly_df = hourly_df.set_index("ds")
    df_15 = hourly_df.resample("15min").ffill()
    df_15 = df_15.reset_index()
    return df_15