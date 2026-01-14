from datetime import datetime
import json
from src.config import settings
from src.utils.logging_config import log

class QualityReporter:
    """
    Generates and saves data quality reports
    """
    
    def __init__(self, spark):
        self.spark = spark

    def save_report(self, layer: str, metrics: dict, success: bool):
        """
        Saves quality metrics to the metadata bucket in MinIO
        """
        report = {
            "timestamp": datetime.now().isoformat(),
            "layer": layer,
            "success": success,
            "metrics": metrics
        }
        
        # In a real scenario, we'd save this as a JSON file to S3
        report_path = f"s3a://metadata/quality_reports/{layer}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            # Simple way to save JSON to S3 via Spark (since we have the session)
            report_df = self.spark.read.json(self.spark.sparkContext.parallelize([json.dumps(report)]))
            report_df.write.mode("overwrite").json(report_path)
            log.info(f"Quality report for {layer} saved to {report_path}")
        except Exception as e:
            log.error(f"Failed to save quality report: {e}")

    def generate_summary_statistics(self, df, layer_name: str):
        """
        Calculates basic summary statistics for a dataframe
        """
        stats = df.summary("count", "mean", "stddev", "min", "max").toPandas().to_dict()
        log.info(f"Summary statistics for {layer_name} generated.")
        return stats
