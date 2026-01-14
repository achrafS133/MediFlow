import mlflow
import mlflow.spark
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql import functions as F
from src.config.spark_config import get_spark_session
from src.config import settings
from src.config.mlflow_config import setup_mlflow
from src.utils.logging_config import log

class ReadmissionPredictor:
    def __init__(self, spark=None):
        self.spark = spark or get_spark_session("Readmission-Predictor")
        setup_mlflow()

    def train(self):
        """
        Trains a model to predict 30-day readmission risk.
        Requires patient history that we'll simulate or derive from clinical data.
        """
        log.info("Starting Readmission Risk Model training")
        
        try:
            # Load silver data
            df = self.spark.read.format("delta").load(settings.SILVER_PATH)
            
            # For demonstration, we'll label records as 'readmitted' (target) 
            # based on a synthetic rule if the column doesn't exist
            if "readmitted" not in df.columns:
                log.info("Generating synthetic labels for readmission model")
                df = df.withColumn("readmitted", 
                                   F.when((F.col("age") > 65) | (F.col("cost") > 15000), 1).otherwise(0))

            with mlflow.start_run(run_name="Readmission-Risk-RF"):
                # Preprocessing
                gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_indexed")
                dept_indexer = StringIndexer(inputCol="department", outputCol="dept_indexed")
                
                assembler = VectorAssembler(
                    inputCols=["age", "gender_indexed", "dept_indexed", "cost"],
                    outputCol="features"
                )
                
                # Model
                rf = RandomForestClassifier(featuresCol="features", labelCol="readmitted")
                
                # Pipeline
                pipeline = Pipeline(stages=[gender_indexer, dept_indexer, assembler, rf])
                
                # Split and Train
                train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
                model = pipeline.fit(train_data)
                
                # Evaluation
                predictions = model.transform(test_data)
                evaluator = BinaryClassificationEvaluator(labelCol="readmitted", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
                roc_auc = evaluator.evaluate(predictions)
                
                log.info(f"Readmission Model AUC: {roc_auc:.4f}")
                mlflow.log_metric("auc", roc_auc)
                mlflow.log_param("model_type", "RandomForestClassifier")
                
                # Save
                model_path = "s3a://metadata/models/readmission_predictor"
                model.write().overwrite().save(model_path)
                mlflow.spark.log_model(model, "readmission_model")
                
                log.info(f"Readmission model saved to {model_path}")
                return model

        except Exception as e:
            log.error(f"Error training readmission model: {str(e)}")
            return None

if __name__ == "__main__":
    predictor = ReadmissionPredictor()
    predictor.train()
