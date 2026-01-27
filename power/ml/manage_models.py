# from power.ml.model_store import save_model
# from power.ml.train import train_region_model, train_state_daily
# from power.ml.weather import fetch_and_save_weather

# STATE_TO_REGION = {
#     "DL": "NR",
#     "MH": "WR",
#     "TN": "SR",
#     "UP": "NR",
#     "AP": "SR",
#     "AR": "NER",
#     "AS": "NER",
#     "BR": "ER",
#     "CH": "NR",
#     "CG": "WR",
#     "GA": "WR",
#     "GJ": "WR",
#     "HR": "NR",
#     "HP": "NR",
#     "JK": "NR",
#     "JH": "ER",
#     "KA": "SR",
#     "KL": "SR",
#     "MN": "NER",
#     "ML": "NER",
#     "MZ": "NER",
#     "MP": "WR",
#     "NL": "NER",
#     "OD": "ER",
#     "PY": "SR",
#     "PB": "NR",
#     "RJ": "WR",
#     "SK": "NER",
#     "TS": "SR",
#     "TR": "NER",
#     "UK": "NR",
#     "WB": "ER",
# }







# def train_all_models(start_date=None, frequency="hourly"):
#     print("Starting region models training...")
#     for region_code in set(STATE_TO_REGION.values()):
#         try:
#             # Try to train region model
#             model = train_region_model(region_code)
#             save_model(f"region_{region_code}.pkl", model)
#             print(f"Saved region model for {region_code}")
#         except ValueError as e:
#             print(f"Region {region_code} missing data: {e}")
#             # Try to fetch weather for all states in this region
#             states = [s for s, r in STATE_TO_REGION.items() if r == region_code]
#             for state_short in states:
#                 print(f"Fetching weather data for {state_short}")
#                 fetch_and_save_weather(state_short, start_date=start_date, frequency=frequency)
#             # Retry training after fetching
#             try:
#                 model = train_region_model(region_code)
#                 save_model(f"region_{region_code}.pkl", model)
#                 print(f"Saved region model for {region_code} after fetching data")
#             except ValueError as e2:
#                 print(f"Still skipping region {region_code}: {e2}")

#     print("Starting state models training...")
#     for state in STATE_TO_REGION.keys():
#         try:
#             model = train_state_daily(state)
#             save_model(f"state_{state}.pkl", model)
#             print(f"Saved state model for {state}")
#         except ValueError as e:
#             print(f"State {state} missing data: {e}")
#             print(f"Fetching weather data for {state}")
#             fetch_and_save_weather(state, start_date=start_date, frequency=frequency)
#             try:
#                 model = train_state_daily(state)
#                 save_model(f"state_{state}.pkl", model)
#                 print(f"Saved state model for {state} after fetching data")
#             except ValueError as e2:
#                 print(f"Still skipping state {state}: {e2}")

#     print("############# All model training done. #############")






from power.ml.model_store import save_model
from power.ml.train import train_region_model, train_state_daily, train_state_5min_model
from power.ml.weather import fetch_and_save_weather
from power.utils.logger import get_logger




# ------------------ LOGGER SETUP ------------------
logger = get_logger(__name__)




# ------------------ STATE TO REGION MAPPING ------------------
STATE_TO_REGION = {
    "DL": "NR", "MH": "WR", "TN": "SR", "UP": "NR", "AP": "SR", "AR": "NER",
    "AS": "NER", "BR": "ER", "CH": "NR", "CG": "WR", "GA": "WR", "GJ": "WR",
    "HR": "NR", "HP": "NR", "JK": "NR", "JH": "ER", "KA": "SR", "KL": "SR",
    "MN": "NER", "ML": "NER", "MZ": "NER", "MP": "WR", "NL": "NER", "OD": "ER",
    "PY": "SR", "PB": "NR", "RJ": "WR", "SK": "NER", "TS": "SR", "TR": "NER",
    "UK": "NR", "WB": "ER",
}







# ------------------ TRAIN ALL MODELS ------------------
def train_all_models(start_date=None, frequency="hourly"):
    # ---------- REGION MODELS ----------
    logger.info("Starting REGION models training")
    for region_code in set(STATE_TO_REGION.values()):
        try:
            model = train_region_model(region_code)
            save_model(f"region_{region_code}.pkl", model)
            logger.info("Saved REGION model for %s", region_code)
        except ValueError as e:
            logger.warning("Region %s missing data: %s", region_code, e)
            # Fetch weather for all states in region
            states = [s for s, r in STATE_TO_REGION.items() if r == region_code]
            for state_short in states:
                logger.info("Fetching weather for %s", state_short)
                fetch_and_save_weather(state_short, start_date=start_date, frequency=frequency)
            # Retry after fetching weather
            try:
                model = train_region_model(region_code)
                save_model(f"region_{region_code}.pkl", model)
                logger.info("Saved REGION model for %s after fetching data", region_code)
            except ValueError as e2:
                logger.error("Still skipping REGION %s: %s", region_code, e2)




    # ---------- STATE DAILY MODELS ----------
    logger.info("\nStarting STATE DAILY models training")
    for state in STATE_TO_REGION.keys():
        try:
            model = train_state_daily(state)
            save_model(f"state_daily_{state}.pkl", model)
            logger.info("Saved DAILY state model for %s", state)
        except ValueError as e:
            logger.warning("State %s missing DAILY data: %s", state, e)
            logger.info("Fetching weather for %s", state)
            fetch_and_save_weather(state, start_date=start_date, frequency=frequency)
            try:
                model = train_state_daily(state)
                save_model(f"state_daily_{state}.pkl", model)
                logger.info("Saved DAILY state model for %s after fetching data", state)
            except ValueError as e2:
                logger.error("Still skipping DAILY state %s: %s", state, e2)






    # ---------- STATE 5-MINUTE MODELS ----------
    logger.info("\nStarting STATE 5-MINUTE models training")
    for state in STATE_TO_REGION.keys():
        try:
            model = train_state_5min_model(state)
            save_model(f"state_5min_{state}.pkl", model)
            logger.info("Saved 5-MINUTE state model for %s", state)
        except ValueError as e:
            logger.warning("5-MINUTE model missing for %s: %s", state, e)
            logger.info("Fetching weather for %s", state)
            fetch_and_save_weather(state, start_date=start_date, frequency=frequency)
            try:
                model = train_state_5min_model(state)
                save_model(f"state_5min_{state}.pkl", model)
                logger.info("Saved 5-MINUTE state model for %s after fetching data", state)
            except ValueError as e2:
                logger.error("Still skipping 5-MINUTE state %s: %s", state, e2)



    logger.info("\n############# ---> ALL MODEL TRAINING COMPLETED <--- ############")
