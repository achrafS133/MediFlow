import requests
import json

BASE_URL = "http://localhost:8000"

def test_api_flow():
    print("--- 🔐 MediFlow API V2 Test Flow ---")
    
    # 1. Get JWT Token
    print("\n1. Requesting Access Token...")
    login_resp = requests.post(f"{BASE_URL}/token")
    if login_resp.status_code != 200:
        print(f"❌ Failed to get token: {login_resp.text}")
        return
    
    token = login_resp.json()["access_token"]
    print(f"✅ Token received: {token[:20]}...")
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # 2. Test Single Prediction
    print("\n2. Testing Single Prediction...")
    patient = {
        "age": 45,
        "gender": "F",
        "department": "CARDIOLOGY",
        "diagnosis_code": "I10"
    }
    
    pred_resp = requests.post(f"{BASE_URL}/predict", json=patient, headers=headers)
    if pred_resp.status_code == 200:
        print(f"✅ Prediction Success: {pred_resp.json()}")
    else:
        print(f"❌ Prediction Failed: {pred_resp.text}")

    # 3. Test Batch Prediction
    print("\n3. Testing Batch Prediction...")
    batch_data = {
        "patients": [
            {"age": 30, "gender": "M", "department": "GENERAL", "diagnosis_code": "A1"},
            {"age": 72, "gender": "F", "department": "ONCOLOGY", "diagnosis_code": "C1"}
        ]
    }
    
    batch_resp = requests.post(f"{BASE_URL}/predict/batch", json=batch_data, headers=headers)
    if batch_resp.status_code == 200:
        print(f"✅ Batch Success: {batch_resp.json()}")
    else:
        print(f"❌ Batch Failed: {batch_resp.text}")

    # 4. Test Readmission Risk
    print("\n4. Testing Readmission Risk...")
    readmit_resp = requests.post(f"{BASE_URL}/predict/readmission", json=patient, headers=headers)
    if readmit_resp.status_code == 200:
        print(f"✅ Readmission Risk: {readmit_resp.json()}")
    else:
        print(f"❌ Readmission Failed: {readmit_resp.text}")

if __name__ == "__main__":
    test_api_flow()
