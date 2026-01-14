import requests
import json

def test_prediction():
    url = "http://localhost:8000/predict"
    payload = {
        "age": 45,
        "gender": "F",
        "department": "CARDIOLOGY",
        "diagnosis_code": "I10"
    }
    
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("Successfully received prediction:")
            print(json.dumps(response.json(), indent=2))
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    # Note: This is intended to be run when the container is UP
    test_prediction()
