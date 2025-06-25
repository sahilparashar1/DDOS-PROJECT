from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import pandas as pd
import numpy as np
from typing import Dict, Any
import logging
import time

from producer import MAX_WORKERS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="DDoS Detection ML API", version="1.0")

ARTIFACT_PATH = 'LR_model_artifacts.pkl'
CLASS_MAP = {
    0: "Attack",
    1: "Benign",
    2: "Suspicious"
}

# Global variables for model components
model = None
scaler = None
feature_order = None
model_classes = None

def load_model():
    """Load model once at startup"""
    global model, scaler, feature_order, model_classes
    try:
        logger.info("Loading model artifact...")
        start_time = time.time()
        
        artifact = joblib.load(ARTIFACT_PATH)
        model = artifact['model']
        scaler = artifact['scaler']
        feature_order = list(artifact['columns'])  # Convert to list for better compatibility
        model_classes = model.classes_
        
        load_time = time.time() - start_time
        logger.info(f"Model loaded successfully in {load_time:.3f} seconds")
        logger.info(f"Model type: {type(model).__name__}")
        logger.info(f"Number of features: {len(feature_order)}")
        
        return True
    except Exception as e:
        logger.error(f"Error loading model artifact: {e}")
        return False

# Load model at startup
if not load_model():
    logger.error("Failed to load model. API will not function properly.")

class FlowInput(BaseModel):
    flow: Dict[str, Any]

@app.post("/predict")
async def predict_flow(data: FlowInput):
    """Predict DDoS attack for a single flow"""
    if model is None or scaler is None:
        raise HTTPException(status_code=500, detail="Model not loaded.")
    
    start_time = time.time()
    
    try:
        flow_data = data.flow
        
        # Create DataFrame and ensure all required features are present
        live_flow_df = pd.DataFrame([flow_data])
        for col in feature_order:
            if col not in live_flow_df.columns:
                live_flow_df[col] = 0
        
        # Reorder columns to match training data
        live_flow_df = live_flow_df[feature_order]
        
        # Handle infinite values and NaN
        live_flow_df = live_flow_df.replace([float('inf'), float('-inf')], 0)
        live_flow_df = live_flow_df.fillna(0)
        
        # Scale features
        scaled_features = scaler.transform(live_flow_df)
        
        # Get prediction and probabilities
        probabilities = model.predict_proba(scaled_features)[0]
        predicted_index = np.argmax(probabilities)
        prediction_label = CLASS_MAP.get(predicted_index, "Unknown")
        confidence_score = float(probabilities[predicted_index])
        
        # Create probability dictionary
        all_probabilities = {}
        for cls, prob in zip(model_classes, probabilities):
            all_probabilities[CLASS_MAP.get(cls, f"Unknown_{cls}")] = float(prob)
        
        # Calculate prediction time
        prediction_time = time.time() - start_time
        
        return {
            "prediction": prediction_label,
            "confidence": confidence_score,
            "all_probabilities": all_probabilities,
            "prediction_time_ms": round(prediction_time * 1000, 2)
        }
        
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=400, detail=f"Prediction error: {e}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy" if model is not None else "unhealthy",
        "model_loaded": model is not None,
        "scaler_loaded": scaler is not None,
        "feature_count": len(feature_order) if feature_order is not None else 0
    }

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "DDoS Detection ML API",
        "version": "1.0",
        "endpoints": {
            "POST /predict": "Predict DDoS attack for a single flow",
            "GET /health": "Health check",
            "GET /docs": "API documentation"
        }
    }

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting DDoS Detection ML API...")
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000, 
        reload=False,  # Set to False for production
        log_level="info",
    )
