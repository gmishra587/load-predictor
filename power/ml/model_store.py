import os
import joblib

PATH = "./power/ml/models"
def save_model(filename: str, model):
    os.makedirs(PATH, exist_ok=True)
    joblib.dump(model, f"./{PATH}/{filename}")

def load_model(filename: str):
    return joblib.load(f"./{PATH}/{filename}")
