from pyspark.ml import PipelineModel
from src.config.spark_config import get_spark_session
from src.utils.logging_config import log

class PredictionService:
    """
    Service for loading the trained model and making predictions
    """
    
    def __init__(self, spark=None):
        self.spark = spark or get_spark_session("Prediction-Service")
        self.model = None
        self.model_path = "s3a://metadata/models/cost_predictor"

    def load_model(self):
        """Loads the model from S3/MinIO"""
        try:
            self.model = PipelineModel.load(self.model_path)
            log.info(f"Model loaded successfully from {self.model_path}")
            return True
        except Exception as e:
            log.error(f"Failed to load model: {e}")
            return False

    def predict(self, patient_data_list):
        """
        Makes predictions for a list of patient features.
        Expected format: [{"age": 30, "gender": "M", "department": "CARDIOLOGY", "diagnosis_code": "I10"}]
        """
        if not self.model and not self.load_model():
            return None
        
        # Create Spark DF from input data
        input_df = self.spark.createDataFrame(patient_data_list)
        
        # Run prediction
        predictions = self.model.transform(input_df)
        
        # Extract results
        results = predictions.select("prediction").collect()
        return [row.prediction for row in results]
