import mlflow
import mlflow.spark
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from src.config.spark_config import get_spark_session
from src.config import settings
from src.config.mlflow_config import setup_mlflow
from src.utils.logging_config import log

class MLPipeline:
    def __init__(self, spark=None):
        self.spark = spark or get_spark_session("ML-Pipeline")
        setup_mlflow()

    def train_cost_predictor(self):
        """
        Trains a model to predict treatment costs based on patient features.
        """
        log.info("Starting ML Model Training with MLflow tracking")
        
        try:
            # 1. Load data from Silver layer
            df = self.spark.read.format("delta").load(settings.SILVER_PATH)
            
            with mlflow.start_run(run_name="Cost-Prediction-RF"):
                # 2. Preprocessing
                gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_indexed")
                dept_indexer = StringIndexer(inputCol="department", outputCol="dept_indexed")
                diag_indexer = StringIndexer(inputCol="diagnosis_code", outputCol="diag_indexed")
                
                assembler = VectorAssembler(
                    inputCols=["age", "gender_indexed", "dept_indexed", "diag_indexed"],
                    outputCol="features"
                )
                
                # 3. Model
                num_trees = 20
                rf = RandomForestRegressor(featuresCol="features", labelCol="cost", numTrees=num_trees)
                
                # Log Params
                mlflow.log_param("numTrees", num_trees)
                mlflow.log_param("features", ["age", "gender", "department", "diagnosis_code"])
                
                # 4. Pipeline
                pipeline = Pipeline(stages=[gender_indexer, dept_indexer, diag_indexer, assembler, rf])
                
                # 5. Split and Train
                train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
                model = pipeline.fit(train_data)
                
                # 6. Evaluation
                predictions = model.transform(test_data)
                evaluator = RegressionEvaluator(labelCol="cost", predictionCol="prediction", metricName="rmse")
                rmse = evaluator.evaluate(predictions)
                
                log.info(f"Model training completed. RMSE: {rmse:.4f}")
                mlflow.log_metric("rmse", rmse)
                
                # 7. Save Model & Log to MLflow
                model_path = "s3a://metadata/models/cost_predictor"
                model.write().overwrite().save(model_path)
                mlflow.spark.log_model(model, "cost_predictor_model")
                
                log.info(f"Model saved to {model_path} and logged to MLflow")
                
                return model

        except Exception as e:
            log.error(f"Error during ML pipeline: {str(e)}")
            return None

if __name__ == "__main__":
    ml_pipeline = MLPipeline()
    ml_pipeline.train_cost_predictor()
