from src.ingestion.bronze_ingestion import BronzeIngestor
from src.transformation.silver_transform import SilverTransformer
from src.transformation.gold_aggregation import GoldAggregator
from src.quality.data_validator import DataValidator
from src.quality.quality_reporter import QualityReporter
from src.quality.anomaly_detector import AnomalyDetector
from src.ml.model_trainer import MLPipeline
from src.ml.prediction_service import PredictionService
from src.utils.logging_config import log
from src.config.spark_config import get_spark_session
from pyspark.sql import functions as F
import time

class ETLOrchestrator:
    def __init__(self):
        self.spark = get_spark_session("ETL-Orchestrator")
        self.bronze = BronzeIngestor(self.spark)
        self.silver = SilverTransformer(self.spark)
        self.gold = GoldAggregator(self.spark)
        self.validator = DataValidator()
        self.reporter = QualityReporter(self.spark)
        self.anomaly_detector = AnomalyDetector(self.spark)
        self.ml = MLPipeline(self.spark)
        self.predictor = PredictionService(self.spark)

    def run_full_pipeline(self, source_path, source_system):
        """
        Executes the full Medallion pipeline.
        """
        start_time = time.time()
        log.info("🚀 Starting MediFlow Full ETL Pipeline")
        
        # 1. Bronze Layer
        success = self.bronze.ingest_raw_data(source_path, source_system)
        if not success:
            log.error("❌ Pipeline failed at Bronze layer")
            return False
            
        # 2. Silver Layer
        success = self.silver.clean_and_normalize()
        if not success:
            log.error("❌ Pipeline failed at Silver layer")
            return False
            
        # 2.1 Anomaly Detection (Silver Layer)
        log.info("🔍 Running anomaly detection on Silver layer")
        silver_df = self.spark.read.format("delta").load("s3a://silver/healthcare")
        anomalies_df = self.anomaly_detector.detect_statistical_anomalies(silver_df, "cost")
        
        # Log anomaly summary
        anomaly_count = anomalies_df.filter(F.col("cost_is_anomaly") == True).count()
        if anomaly_count > 0:
            log.warning(f"🚨 Detected {anomaly_count} cost anomalies in Silver layer")
        else:
            log.info("✅ No cost anomalies detected in Silver layer")
            
        # 3. Gold Layer
        success = self.gold.create_patient_summary()
        if not success:
            log.error("❌ Pipeline failed at Gold layer")
            return False
            
        # 4. Data Quality & Reporting
        log.info("📊 Running final data quality checks and reporting")
        silver_df = self.spark.read.format("delta").load("s3a://silver/healthcare")
        gold_df = self.spark.read.format("delta").load("s3a://gold/healthcare")
        
        silver_valid = self.validator.validate_silver_layer(silver_df)
        gold_valid = self.validator.validate_gold_layer(gold_df)
        
        self.reporter.save_report("silver", {}, silver_valid)
        self.reporter.save_report("gold", {}, gold_valid)
            
        # 5. ML Training
        log.info("🤖 Starting ML Training")
        self.ml.train_cost_predictor()
        
        # 6. Sample Prediction
        log.info("🔮 Running sample prediction test")
        sample_data = [{"age": 45, "gender": "F", "department": "CARDIOLOGY", "diagnosis_code": "I10"}]
        prediction = self.predictor.predict(sample_data)
        log.info(f"Test prediction for age 45, F, Cardiology, I10: ${prediction[0]:.2f}")
        
        end_time = time.time()
        duration = end_time - start_time
        log.info(f"✅ Full ETL Pipeline completed successfully in {duration:.2f} seconds")
        return True

if __name__ == "__main__":
    orchestrator = ETLOrchestrator()
    orchestrator.run_full_pipeline("/app/data/raw/patients_synthetic.csv", "EHR_SYSTEM_A")
