from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import pandas as pd
import numpy as np
from typing import Dict, Any

app = FastAPI()

ARTIFACT_PATH = 'LR_model_artifacts.pkl'
CLASS_MAP = {
    0: "Attack",
    1: "Benign",
    2: "Suspicious"
}

# Load model artifact at startup
try:
    artifact = joblib.load(ARTIFACT_PATH)
    model = artifact['model']
    scaler = artifact['scaler']
    feature_order = artifact['columns']
    model_classes = model.classes_
except Exception as e:
    artifact = None
    model = None
    scaler = None
    feature_order = None
    model_classes = None
    print(f"Error loading model artifact: {e}")

class FlowInput(BaseModel):
    flow: Dict[str, Any]

@app.post("/predict")
def predict_flow(data: FlowInput):
    if model is None or scaler is None or feature_order is None or len(feature_order) == 0:
        raise HTTPException(status_code=500, detail="Model not loaded.")
    flow_data = data.flow
    try:
        live_flow_df = pd.DataFrame([flow_data])
        for col in feature_order:
            if col not in live_flow_df.columns:
                live_flow_df[col] = 0
        live_flow_df = live_flow_df[feature_order]
        live_flow_df = live_flow_df.replace([float('inf'), float('-inf')], 0)
        live_flow_df = live_flow_df.fillna(0)
        scaled_features = scaler.transform(live_flow_df)
        probabilities = model.predict_proba(scaled_features)[0]
        predicted_index = np.argmax(probabilities)
        prediction_label = CLASS_MAP.get(predicted_index, "Unknown")
        confidence_score = float(probabilities[predicted_index])
        all_probabilities = {CLASS_MAP.get(cls, f"Unknown_{cls}"): float(prob) for cls, prob in zip(model_classes, probabilities)}
        return {
            "prediction": prediction_label,
            "confidence": confidence_score,
            "all_probabilities": all_probabilities
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Prediction error: {e}")
