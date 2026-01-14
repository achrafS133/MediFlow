import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta
import sys
import argparse

def generate_medical_data(num_records=1000, output_path="data/raw/patients_synthetic.csv"):
    """
    Generates synthetic medical data for testing.
    """
    print(f"Generating {num_records} synthetic records...")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    departments = ["CARDIOLOGY", "NEUROLOGY", "ONCOLOGY", "PEDIATRICS", "GENERAL MEDICINE"]
    diagnosis_codes = ["I10", "E11.9", "F32.9", "M54.5", "J06.9", "Z00.00"]
    genders = ["M", "F", "O"]
    
    data = {
        "patient_id": [f"PAT_{i:07d}" for i in range(num_records)],
        "name": [f"Patient_{i}" for i in range(num_records)],
        "age": [random.randint(0, 100) for _ in range(num_records)],
        "gender": [random.choice(genders) for _ in range(num_records)],
        "department": [random.choice(departments) for _ in range(num_records)],
        "diagnosis_code": [random.choice(diagnosis_codes) for _ in range(num_records)],
        "admission_date": [(datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d") for _ in range(num_records)],
        "blood_pressure": [f"{random.randint(110, 150)}/{random.randint(70, 95)}" for _ in range(num_records)],
        "cost": [round(random.uniform(500, 15000), 2) for _ in range(num_records)]
    }
    
    # Introduce some anomalies (age > 150, very high cost)
    for _ in range(int(num_records * 0.01)): # 1% anomalies
        idx = random.randint(0, num_records - 1)
        data["age"][idx] = random.randint(150, 250)
        data["cost"][idx] = random.uniform(50000, 100000)
    
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    print(f"Data saved to {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic medical data.")
    parser.add_argument("--records", type=int, default=2000, help="Number of records to generate")
    args = parser.parse_args()
    
    generate_medical_data(args.records)
