import json
from datetime import datetime
from src.config.spark_config import get_spark_session
from src.utils.logging_config import log

class LineageTracker:
    """
    Simple lineage tracker for Delta Lake transformations.
    Captures source, target, and transformation metadata.
    """
    def __init__(self, spark=None):
        self.spark = spark or get_spark_session("Lineage-Tracker")
        self.lineage_path = "s3a://metadata/lineage/history.json"

    def record_transformation(self, source, target, transform_type, description):
        """
        Records a transformation step.
        """
        entry = {
            "timestamp": datetime.now().isoformat(),
            "source": source,
            "target": target,
            "type": transform_type,
            "description": description
        }
        
        log.info(f"Recording lineage: {source} -> {target} ({transform_type})")
        
        try:
            new_lineage_df = self.spark.createDataFrame([entry])
            new_lineage_df.write.mode("append").format("json").save(self.lineage_path)
            
        except Exception as e:
            log.error(f"Failed to record lineage: {str(e)}")

    def get_lineage_history(self):
        """
        Retrieves the full lineage history.
        """
        try:
            return self.spark.read.json(self.lineage_path)
        except Exception:
            return None
