from power.ml.manage_models import STATE_TO_REGION
from power.ml.model_store import load_model, save_model
from power.ml.weather import fetch_and_save_weather
from power.ml.train import train_region_model, train_state_daily



def background_work(state_short: str, start_date: str, frequency: str):
    # Weather fetch & save
    weather_data = fetch_and_save_weather(state_short, start_date, frequency=frequency)

    # Load pre-trained models safely
    region_code = STATE_TO_REGION[state_short]

    try:
        region_model = load_model(f"region_{region_code}.pkl")
    except FileNotFoundError:
        print(f"Region model {region_code} not found, training now...")
        region_model = train_region_model(region_code)
        save_model(f"region_{region_code}.pkl", region_model)

    try:
        state_model = load_model(f"state_{state_short}.pkl")
    except FileNotFoundError:
        print(f"State model {state_short} not found, training now...")
        state_model = train_state_daily(state_short)
        save_model(f"state_{state_short}.pkl", state_model)

    return weather_data, region_model, state_model
