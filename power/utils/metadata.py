import holidays
from datetime import date
import pandas as pd

INDIA_HOLIDAYS = holidays.India()




def get_season(month: int) -> str:
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "summer"
    elif month in [6, 7, 8, 9]:
        return "monsoon"
    else:
        return "post-monsoon"




def day_metadata(d: date):
    return {
        "season": get_season(d.month),
        "weekday": d.strftime("%A"),
        "is_weekend": d.weekday() >= 5,
        "is_holiday": d in INDIA_HOLIDAYS
    }





def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ds"] = pd.to_datetime(df["ds"])

    df["is_weekend"] = (df["ds"].dt.weekday >= 5).astype(int)
    df["is_holiday"] = df["ds"].dt.date.apply(lambda d: int(d in INDIA_HOLIDAYS))

    month = df["ds"].dt.month

    df["season"] = 0
    df.loc[month.isin([12, 1, 2]), "season"] = 0  # winter
    df.loc[month.isin([3, 4, 5]), "season"] = 1   # summer
    df.loc[month.isin([6, 7, 8, 9]), "season"] = 2 # monsoon
    df.loc[month.isin([10, 11]), "season"] = 3    # post-monsoon

    return df
