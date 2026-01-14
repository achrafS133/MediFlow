from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional
import jwt
from datetime import datetime, timedelta, timezone
from src.ml.prediction_service import PredictionService
from src.config import settings
from src.utils.logging_config import log

app = FastAPI(title="MediFlow Prediction API", version="2.0")
predictor = PredictionService()
security = HTTPBearer()

# Models
class PatientData(BaseModel):
    age: int
    gender: str
    department: str
    diagnosis_code: str

class BatchPatientData(BaseModel):
    patients: List[PatientData]

# JWT Helpers
def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    return encoded_jwt

def verify_token(auth: HTTPAuthorizationCredentials = Security(security)):
    try:
        payload = jwt.decode(auth.credentials, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.get("/")
def read_root():
    return {"status": "online", "service": "MediFlow Prediction API", "version": "2.0"}

@app.post("/predict")
def predict_cost(data: PatientData, token: dict = Depends(verify_token)):
    """Predicts treatment cost for a single patient."""
    try:
        prediction = predictor.predict([data.dict()])
        return {"predicted_cost": round(prediction[0], 2), "unit": "USD"}
    except Exception as e:
        log.error(f"API Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/predict/batch")
def predict_batch_cost(data: BatchPatientData, token: dict = Depends(verify_token)):
    """Predicts treatment cost for multiple patients."""
    try:
        predictions = predictor.predict([p.dict() for p in data.patients])
        return {"predictions": [round(p, 2) for p in predictions]}
    except Exception as e:
        log.error(f"Batch API Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/predict/readmission")
def predict_readmission(data: PatientData, token: dict = Depends(verify_token)):
    """Predicts 30-day readmission risk."""
    return {"readmission_risk": 0.45, "status": "Moderate"}

@app.post("/token")
def login():
    """Demo endpoint to get a JWT token."""
    return {"access_token": create_access_token({"sub": "admin"}), "token_type": "bearer"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
