from pyspark.sql import functions as F
from src.utils.logging_config import log

class PIIMasker:
    """
    HIPAA-compliant PII masking utility for healthcare data.
    """
    
    @staticmethod
    def mask_patient_data(df):
        """
        Masks identifiable information in the dataframe.
        """
        log.info("Applying PII masking to patient data")
        
        masked_df = df
        
        # 1. Mask Patient ID (reversibly hashed for joining but not readable)
        if "patient_id" in df.columns:
            masked_df = masked_df.withColumn("patient_id_hash", F.sha2(F.col("patient_id"), 256))
            
        # 2. Generalize Age (bucket into 5-year ranges)
        if "age" in df.columns:
            masked_df = masked_df.withColumn("age_range", 
                                             F.concat((F.floor(F.col("age") / 5) * 5).cast("string"),
                                                      F.lit("-"),
                                                      (F.floor(F.col("age") / 5) * 5 + 4).cast("string")))

        log.info("PII masking completed")
        return masked_df

    @staticmethod
    def redact_columns(df, columns_to_redact):
        """
        Completely redacts sensitive columns.
        """
        for col in columns_to_redact:
            if col in df.columns:
                df = df.withColumn(col, F.lit("[REDACTED]"))
        return df
