from datetime import timezone
import pandas as pd
from django.core.exceptions import ValidationError
from django.db import transaction
import pytz
from power.models import RegionHourlyLoad, StateDailyLoad


REGION_COLUMN_MAP = {
    "NR": "Northen Region Hourly Demand",
    "WR": "Western Region Hourly Demand",
    "ER": "Eastern Region Hourly Demand",
    "SR": "Southern Region Hourly Demand",
    "NER": "North-Eastern Region Hourly Demand",
}




STATE_SHORT_MAP = {
    "Andhra Pradesh": "AP",
    "Arunachal Pradesh": "AR",
    "Assam": "AS",
    "Bihar": "BR",
    "Chandigarh": "CH",
    "Chhattisgarh": "CG",
    "DD": "DD",
    "Delhi": "DL",
    "DNH": "DNH",
    "DVC": "DVC",
    "Essar steel": "ESS",
    "Goa": "GA",
    "Gujarat": "GJ",
    "Haryana": "HR",
    "HP": "HP",
    "J&K": "JK",
    "Jharkhand": "JH",
    "Karnataka": "KA",
    "Kerala": "KL",
    "Maharashtra": "MH",
    "Manipur": "MN",
    "Meghalaya": "ML",
    "Mizoram": "MZ",
    "MP": "MP",
    "Nagaland": "NL",
    "Odisha": "OD",
    "Pondy": "PY",
    "Punjab": "PB",
    "Rajasthan": "RJ",
    "Sikkim": "SK",
    "Tamil Nadu": "TN",
    "Telangana": "TS",
    "Tripura": "TR",
    "UP": "UP",
    "Uttarakhand": "UK",
    "West Bengal": "WB",
}

SKIP_COLUMNS = {"Dates", "Total Consumption", "Unnamed: 0"}








def save_power_data_from_xlsx(file) -> int:
    filename = file.name.lower()

    if filename.endswith(".csv"):
        df = pd.read_csv(file)
        file_type = "CSV"
    elif filename.endswith(".xlsx") or filename.endswith(".xls"):
        df = pd.read_excel(file)
        file_type = "XLSX"
    else:
        raise ValidationError("Unsupported file type")

    if df.empty:
        raise ValidationError("Uploaded file is empty")

    if file_type == "CSV" and "Dates" in df.columns:
        return save_state_daily_load_from_csv(df)

    if file_type == "XLSX" and "datetime" in df.columns:
        return save_region_hourly_load_from_xlsx(df)

    raise ValidationError("Unsupported file format / columns")









def save_region_hourly_load_from_xlsx(df) -> int:
    IST = pytz.timezone("Asia/Kolkata")
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    records = []

    for _, row in df.iterrows():
        dt = row["datetime"]

        if pd.isna(dt):
            continue
        
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, IST)

        for region_code, column_name in REGION_COLUMN_MAP.items():
            if column_name not in df.columns:
                continue

            load = row[column_name]

            if pd.isna(load):
                continue

            records.append(
                RegionHourlyLoad(
                    region=region_code,
                    datetime=dt,
                    load_mw=float(load)
                )
            )

    if not records:
        raise ValidationError("No region-wise data found")

    with transaction.atomic():
        RegionHourlyLoad.objects.bulk_create(records, batch_size=1000)

    return len(records)
















def save_state_daily_load_from_csv(df) -> int:
    if "Dates" not in df.columns:
        raise ValidationError("Missing 'Dates' column")

    df["Dates"] = pd.to_datetime(df["Dates"], errors="coerce").dt.date

    records = []

    for _, row in df.iterrows():
        date = row["Dates"]

        if pd.isna(date):
            continue

        for col in df.columns:
            if col in SKIP_COLUMNS:
                continue

            short_state = STATE_SHORT_MAP.get(col)

            if not short_state:
                continue

            value = row[col]

            if pd.isna(value):
                continue

            records.append(
                StateDailyLoad(
                    state=short_state,
                    date=date,
                    energy_mu=float(value)
                )
            )

    if not records:
        raise ValidationError("No valid state daily load data")

    with transaction.atomic():
        StateDailyLoad.objects.bulk_create(records, batch_size=2000)

    return len(records)

