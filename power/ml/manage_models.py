from power.ml.model_store import save_model
from power.ml.train import train_region_model, train_state_daily
from power.ml.weather import fetch_and_save_weather

STATE_TO_REGION = {
    "DL": "NR",
    "MH": "WR",
    "TN": "SR",
    "UP": "NR",
    "AP": "SR",
    "AR": "NER",
    "AS": "NER",
    "BR": "ER",
    "CH": "NR",
    "CG": "WR",
    "GA": "WR",
    "GJ": "WR",
    "HR": "NR",
    "HP": "NR",
    "JK": "NR",
    "JH": "ER",
    "KA": "SR",
    "KL": "SR",
    "MN": "NER",
    "ML": "NER",
    "MZ": "NER",
    "MP": "WR",
    "NL": "NER",
    "OD": "ER",
    "PY": "SR",
    "PB": "NR",
    "RJ": "WR",
    "SK": "NER",
    "TS": "SR",
    "TR": "NER",
    "UK": "NR",
    "WB": "ER",
}







def train_all_models(start_date=None, frequency="hourly"):
    print("Starting region models training...")
    for region_code in set(STATE_TO_REGION.values()):
        try:
            # Try to train region model
            model = train_region_model(region_code)
            save_model(f"region_{region_code}.pkl", model)
            print(f"Saved region model for {region_code}")
        except ValueError as e:
            print(f"Region {region_code} missing data: {e}")
            # Try to fetch weather for all states in this region
            states = [s for s, r in STATE_TO_REGION.items() if r == region_code]
            for state_short in states:
                print(f"Fetching weather data for {state_short}")
                fetch_and_save_weather(state_short, start_date=start_date, frequency=frequency)
            # Retry training after fetching
            try:
                model = train_region_model(region_code)
                save_model(f"region_{region_code}.pkl", model)
                print(f"Saved region model for {region_code} after fetching data")
            except ValueError as e2:
                print(f"Still skipping region {region_code}: {e2}")

    print("Starting state models training...")
    for state in STATE_TO_REGION.keys():
        try:
            model = train_state_daily(state)
            save_model(f"state_{state}.pkl", model)
            print(f"Saved state model for {state}")
        except ValueError as e:
            print(f"State {state} missing data: {e}")
            print(f"Fetching weather data for {state}")
            fetch_and_save_weather(state, start_date=start_date, frequency=frequency)
            try:
                model = train_state_daily(state)
                save_model(f"state_{state}.pkl", model)
                print(f"Saved state model for {state} after fetching data")
            except ValueError as e2:
                print(f"Still skipping state {state}: {e2}")

    print("############# All model training done. #############")
