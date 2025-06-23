from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import pandas as pd
import numpy as np
from typing import Dict, Any, List
import time
import uuid
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

ARTIFACT_PATH = 'LR_model_artifacts.pkl'
CLASS_MAP = {
    0: "Attack",
    1: "Benign",
    2: "Suspicious"
}

# Global variables for model
model = None
scaler = None
feature_order = None
model_classes = None

def load_model():
    """Load model artifact at startup."""
    global model, scaler, feature_order, model_classes
    
    try:
        artifact = joblib.load(ARTIFACT_PATH)
        model = artifact['model']
        scaler = artifact['scaler']
        feature_order = artifact['columns']
        model_classes = model.classes_
        
        logger.info(f"Model loaded successfully with {len(feature_order)} features")
        
    except Exception as e:
        logger.error(f"Error loading model artifact: {e}")

class FlowInput(BaseModel):
    flow: Dict[str, Any]

class BatchFlowInput(BaseModel):
    flows: List[Dict[str, Any]]

def preprocess_flows_batch(flows_data: List[Dict[str, Any]]) -> np.ndarray:
    """Ultra-fast batch preprocessing."""
    try:
        # Create DataFrame efficiently
        live_flow_df = pd.DataFrame(flows_data)
        
        # Handle missing features efficiently - only add missing columns
        missing_cols = set(feature_order) - set(live_flow_df.columns)
        for col in missing_cols:
            live_flow_df[col] = 0
        
        # Select only required features and reorder
        live_flow_df = live_flow_df[feature_order]
        
        # Clean data efficiently - vectorized operations
        # Replace string values with 0 or appropriate numeric values
        for col in live_flow_df.columns:
            if live_flow_df[col].dtype == 'object':
                # Handle categorical columns
                if col == 'handshake_state':
                    # Map handshake states to numeric values
                    live_flow_df[col] = live_flow_df[col].map({
                        'not a complete handshake': -1,
                    }).fillna(0)
                elif col == 'label':
                    # Map labels to numeric values
                    live_flow_df[col] = live_flow_df[col].map({
                        'Benign': 0,
                        'Attack': 1,
                        'Suspicious': 2,
                        'Unknown': 0
                    }).fillna(0)
                else:
                    # For other string columns, try to convert to numeric, fill with 0 if fails
                    live_flow_df[col] = pd.to_numeric(live_flow_df[col], errors='coerce').fillna(0)
        
        # Replace infinite values and NaN
        live_flow_df = live_flow_df.replace([np.inf, -np.inf], 0)
        live_flow_df = live_flow_df.fillna(0)
        
        # Ensure all values are numeric
        live_flow_df = live_flow_df.astype(float)
        
        return live_flow_df.values
        
    except Exception as e:
        logger.error(f"Batch preprocessing error: {e}")
        raise

def predict_batch_flows(flows_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ultra-fast batch prediction."""
    try:
        # Preprocess entire batch at once
        features = preprocess_flows_batch(flows_data)
        
        # Scale features
        scaled_features = scaler.transform(features)
        
        # Get predictions for entire batch
        probabilities = model.predict_proba(scaled_features)
        predicted_indices = np.argmax(probabilities, axis=1)
        
        # Process results
        results = []
        for i, (flow_data, probs, pred_idx) in enumerate(zip(flows_data, probabilities, predicted_indices)):
            prediction_label = CLASS_MAP.get(predicted_indices[i], "Unknown")
            confidence_score = float(probs[predicted_indices[i]])
            
            results.append({
                "prediction_id": str(uuid.uuid4()),
                "prediction": prediction_label,
                "confidence": confidence_score,
                "processing_time_ms": 0  # Will be calculated per batch
            })
        
        return results
        
    except Exception as e:
        logger.error(f"Batch prediction error: {e}")
        raise

@app.on_event("startup")
async def startup_event():
    """Load model at startup."""
    load_model()

@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy" if model is not None else "unhealthy",
        "model_loaded": model is not None,
        "scaler_loaded": scaler is not None,
        "feature_order_loaded": feature_order is not None
    }

@app.post("/predict")
async def predict_flow(data: FlowInput):
    """Single flow prediction endpoint."""
    if model is None:
        raise HTTPException(status_code=500, detail="Model not loaded.")
    
    try:
        # Use batch prediction for single flow
        results = predict_batch_flows([data.flow])
        return results[0]
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Prediction error: {e}")

@app.post("/predict_batch")
async def predict_batch(data: BatchFlowInput):
    """Ultra-fast batch prediction endpoint."""
    if model is None:
        raise HTTPException(status_code=500, detail="Model not loaded.")
    
    batch_start_time = time.time()
    
    try:
        # Process entire batch at once
        results = predict_batch_flows(data.flows)
        
        batch_time = (time.time() - batch_start_time) * 1000
        
        return {
            "batch_id": str(uuid.uuid4()),
            "total_flows": len(data.flows),
            "successful_predictions": len(results),
            "failed_predictions": 0,
            "results": results,
            "errors": [],
            "batch_time_ms": batch_time,
            "avg_time_per_flow_ms": batch_time / len(data.flows) if data.flows else 0
        }
        
    except Exception as e:
        batch_time = (time.time() - batch_start_time) * 1000
        logger.error(f"Batch prediction error: {e}")
        raise HTTPException(status_code=400, detail=f"Batch prediction error: {e}")

@app.get("/model-info")
def get_model_info():
    """Get model information endpoint."""
    if model is None:
        raise HTTPException(status_code=500, detail="Model not loaded")
    
    return {
        "model_type": type(model).__name__,
        "feature_count": len(feature_order),
        "classes": list(CLASS_MAP.values()),
        "model_classes": model_classes.tolist() if hasattr(model_classes, 'tolist') else list(model_classes),
        "artifact_path": ARTIFACT_PATH
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, workers=1) 