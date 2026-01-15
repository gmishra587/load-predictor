# from datetime import timezone
# import pandas as pd
# from django.core.exceptions import ValidationError
# from django.db import transaction
# import pytz
# from power.models import RegionHourlyLoad, StateDailyLoad


# REGION_COLUMN_MAP = {
#     "NR": "Northen Region Hourly Demand",
#     "WR": "Western Region Hourly Demand",
#     "ER": "Eastern Region Hourly Demand",
#     "SR": "Southern Region Hourly Demand",
#     "NER": "North-Eastern Region Hourly Demand",
# }




# STATE_SHORT_MAP = {
#     "Andhra Pradesh": "AP",
#     "Arunachal Pradesh": "AR",
#     "Assam": "AS",
#     "Bihar": "BR",
#     "Chandigarh": "CH",
#     "Chhattisgarh": "CG",
#     "DD": "DD",
#     "Delhi": "DL",
#     "DNH": "DNH",
#     "DVC": "DVC",
#     "Essar steel": "ESS",
#     "Goa": "GA",
#     "Gujarat": "GJ",
#     "Haryana": "HR",
#     "HP": "HP",
#     "J&K": "JK",
#     "Jharkhand": "JH",
#     "Karnataka": "KA",
#     "Kerala": "KL",
#     "Maharashtra": "MH",
#     "Manipur": "MN",
#     "Meghalaya": "ML",
#     "Mizoram": "MZ",
#     "MP": "MP",
#     "Nagaland": "NL",
#     "Odisha": "OD",
#     "Pondy": "PY",
#     "Punjab": "PB",
#     "Rajasthan": "RJ",
#     "Sikkim": "SK",
#     "Tamil Nadu": "TN",
#     "Telangana": "TS",
#     "Tripura": "TR",
#     "UP": "UP",
#     "Uttarakhand": "UK",
#     "West Bengal": "WB",
# }

# SKIP_COLUMNS = {"Dates", "Total Consumption", "Unnamed: 0"}








# def save_power_data_from_xlsx(file) -> int:
#     filename = file.name.lower()

#     if filename.endswith(".csv"):
#         df = pd.read_csv(file)
#         file_type = "CSV"
#     elif filename.endswith(".xlsx") or filename.endswith(".xls"):
#         df = pd.read_excel(file)
#         file_type = "XLSX"
#     else:
#         raise ValidationError("Unsupported file type")

#     if df.empty:
#         raise ValidationError("Uploaded file is empty")

#     if file_type == "CSV" and "Dates" in df.columns:
#         return save_state_daily_load_from_csv(df)

#     if file_type == "XLSX" and "datetime" in df.columns:
#         return save_region_hourly_load_from_xlsx(df)

#     raise ValidationError("Unsupported file format / columns")









# def save_region_hourly_load_from_xlsx(df) -> int:
#     IST = pytz.timezone("Asia/Kolkata")
#     df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

#     records = []

#     for _, row in df.iterrows():
#         dt = row["datetime"]

#         if pd.isna(dt):
#             continue
        
#         if timezone.is_naive(dt):
#             dt = timezone.make_aware(dt, IST)

#         for region_code, column_name in REGION_COLUMN_MAP.items():
#             if column_name not in df.columns:
#                 continue

#             load = row[column_name]

#             if pd.isna(load):
#                 continue

#             records.append(
#                 RegionHourlyLoad(
#                     region=region_code,
#                     datetime=dt,
#                     load_mw=float(load)
#                 )
#             )

#     if not records:
#         raise ValidationError("No region-wise data found")

#     with transaction.atomic():
#         RegionHourlyLoad.objects.bulk_create(records, batch_size=1000)

#     return len(records)
















# def save_state_daily_load_from_csv(df) -> int:
#     if "Dates" not in df.columns:
#         raise ValidationError("Missing 'Dates' column")

#     df["Dates"] = pd.to_datetime(df["Dates"], errors="coerce").dt.date

#     records = []

#     for _, row in df.iterrows():
#         date = row["Dates"]

#         if pd.isna(date):
#             continue

#         for col in df.columns:
#             if col in SKIP_COLUMNS:
#                 continue

#             short_state = STATE_SHORT_MAP.get(col)

#             if not short_state:
#                 continue

#             value = row[col]

#             if pd.isna(value):
#                 continue

#             records.append(
#                 StateDailyLoad(
#                     state=short_state,
#                     date=date,
#                     energy_mu=float(value)
#                 )
#             )

#     if not records:
#         raise ValidationError("No valid state daily load data")

#     with transaction.atomic():
#         StateDailyLoad.objects.bulk_create(records, batch_size=2000)

#     return len(records)






import pandas as pd
import pytz
from itertools import islice
from django.db import transaction
from django.core.exceptions import ValidationError
from power.models import RegionHourlyLoad, StateDailyLoad, StateLoad5Min


# =====================================================
# CONSTANTS
# =====================================================

IST = pytz.timezone("Asia/Kolkata")

REGION_COLUMN_MAP = {
    "NR": "northen region hourly demand",
    "WR": "western region hourly demand",
    "ER": "eastern region hourly demand",
    "SR": "southern region hourly demand",
    "NER": "north-eastern region hourly demand",
}

STATE_SHORT_MAP = {
    "Andhra Pradesh": "AP",
    "Arunachal Pradesh": "AR",
    "Assam": "AS",
    "Bihar": "BR",
    "Chandigarh": "CH",
    "Chhattisgarh": "CG",
    "Delhi": "DL",
    "Goa": "GA",
    "Gujarat": "GJ",
    "Haryana": "HR",
    "Himachal Pradesh": "HP",
    "Jammu & Kashmir": "JK",
    "Jharkhand": "JH",
    "Karnataka": "KA",
    "Kerala": "KL",
    "Maharashtra": "MH",
    "Manipur": "MN",
    "Meghalaya": "ML",
    "Mizoram": "MZ",
    "Madhya Pradesh": "MP",
    "Nagaland": "NL",
    "Odisha": "OD",
    "Puducherry": "PY",
    "Punjab": "PB",
    "Rajasthan": "RJ",
    "Sikkim": "SK",
    "Tamil Nadu": "TN",
    "Telangana": "TS",
    "Tripura": "TR",
    "Uttar Pradesh": "UP",
    "Uttarakhand": "UK",
    "West Bengal": "WB",
}

SHORT_CODES = set(STATE_SHORT_MAP.values())
SKIP_COLUMNS = {"dates", "total consumption", "unnamed: 0"}


# =====================================================
# HELPERS
# =====================================================

def make_ist_aware(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def normalize_state(value):
    if not value:
        return None
    value = str(value).strip()
    if value in SHORT_CODES:
        return value
    return STATE_SHORT_MAP.get(value)


def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk


# =====================================================
# BULK UPSERT HELPERS (SQLite SAFE)
# =====================================================

def bulk_upsert_region(records, chunk_size=400):
    keys = {(r.region, r.datetime) for r in records}
    existing = {}

    for chunk in chunked(list(keys), chunk_size):
        qs = RegionHourlyLoad.objects.filter(
            region__in=[k[0] for k in chunk],
            datetime__in=[k[1] for k in chunk],
        )
        for o in qs:
            existing[(o.region, o.datetime)] = o

    create, update = [], []
    for r in records:
        key = (r.region, r.datetime)
        if key in existing:
            obj = existing[key]
            obj.load_mw = r.load_mw
            update.append(obj)
        else:
            create.append(r)

    with transaction.atomic():
        if create:
            RegionHourlyLoad.objects.bulk_create(create, batch_size=400)
        if update:
            RegionHourlyLoad.objects.bulk_update(update, ["load_mw"], batch_size=400)


def bulk_upsert_state_daily(records, chunk_size=400):
    keys = {(r.state, r.date) for r in records}
    existing = {}

    for chunk in chunked(list(keys), chunk_size):
        qs = StateDailyLoad.objects.filter(
            state__in=[k[0] for k in chunk],
            date__in=[k[1] for k in chunk],
        )
        for o in qs:
            existing[(o.state, o.date)] = o

    create, update = [], []
    for r in records:
        key = (r.state, r.date)
        if key in existing:
            obj = existing[key]
            obj.energy_mu = r.energy_mu
            update.append(obj)
        else:
            create.append(r)

    with transaction.atomic():
        if create:
            StateDailyLoad.objects.bulk_create(create, batch_size=400)
        if update:
            StateDailyLoad.objects.bulk_update(update, ["energy_mu"], batch_size=400)


def bulk_upsert_state_5min(records, chunk_size=400):
    keys = {(r.state, r.datetime) for r in records}
    existing = {}

    for chunk in chunked(list(keys), chunk_size):
        qs = StateLoad5Min.objects.filter(
            state__in=[k[0] for k in chunk],
            datetime__in=[k[1] for k in chunk],
        )
        for o in qs:
            existing[(o.state, o.datetime)] = o

    create, update = [], []
    for r in records:
        key = (r.state, r.datetime)
        if key in existing:
            obj = existing[key]
            obj.load_mw = r.load_mw
            update.append(obj)
        else:
            create.append(r)

    with transaction.atomic():
        if create:
            StateLoad5Min.objects.bulk_create(create, batch_size=400)
        if update:
            StateLoad5Min.objects.bulk_update(update, ["load_mw"], batch_size=400)


# =====================================================
# SAVE FUNCTIONS
# =====================================================

def save_region_hourly_load_from_xlsx(df):
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    records = []

    for _, row in df.iterrows():
        if pd.isna(row["datetime"]):
            continue

        dt = make_ist_aware(row["datetime"])
        for region, col in REGION_COLUMN_MAP.items():
            if col in df.columns and not pd.isna(row[col]):
                records.append(
                    RegionHourlyLoad(region=region, datetime=dt, load_mw=float(row[col]))
                )

    if not records:
        raise ValidationError("No region hourly data found")

    bulk_upsert_region(records)
    return len(records)


def save_state_daily_load_from_csv(df):
    df["dates"] = pd.to_datetime(df["dates"], errors="coerce").dt.date
    records = []

    for _, row in df.iterrows():
        if pd.isna(row["dates"]):
            continue

        for col in df.columns:
            if col in SKIP_COLUMNS:
                continue

            state = normalize_state(col)
            if not state or pd.isna(row[col]):
                continue

            records.append(
                StateDailyLoad(
                    state=state,
                    date=row["dates"],
                    energy_mu=float(row[col]),
                )
            )

    if not records:
        raise ValidationError("No state daily data found")

    bulk_upsert_state_daily(records)
    return len(records)


def save_state_5min_load_from_csv(df):
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    records = []

    for _, row in df.iterrows():
        state = normalize_state(row["state"])
        if not state or pd.isna(row["datetime"]) or pd.isna(row["load"]):
            continue

        records.append(
            StateLoad5Min(
                state=state,
                datetime=make_ist_aware(row["datetime"]),
                load_mw=float(row["load"]),
            )
        )

    if not records:
        raise ValidationError("No 5-minute data found")

    bulk_upsert_state_5min(records)
    return len(records)


# =====================================================
# MAIN UPLOAD FUNCTION
# =====================================================

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

    # 🔥 normalize column names
    df.columns = df.columns.str.strip().str.lower()

    if file_type == "CSV" and {"datetime", "state", "load"}.issubset(df.columns):
        return save_state_5min_load_from_csv(df)

    if file_type == "CSV" and "dates" in df.columns:
        return save_state_daily_load_from_csv(df)

    if file_type == "XLSX" and "datetime" in df.columns:
        return save_region_hourly_load_from_xlsx(df)

    raise ValidationError(f"Unsupported file format / columns: {list(df.columns)}")
