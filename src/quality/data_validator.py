import great_expectations as ge
from pyspark.sql import DataFrame
from src.utils.logging_config import log

class DataValidator:
    """
    Data Validation using Great Expectations for Spark
    """
    
    def __init__(self):
        """Initializes the validator. Currently stateless."""
        pass

    def validate_silver_layer(self, df: DataFrame) -> bool:
        """
        Validates Silver layer data for consistency and quality.
        """
        log.info("Starting Silver layer validation")
        
        # Convert Spark DF to Great Expectations Dataset
        ge_df = ge.dataset.SparkDFDataset(df)
        
        results = []
        
        # 1. Null checks for critical columns
        results.append(ge_df.expect_column_values_to_not_be_null("patient_id"))
        results.append(ge_df.expect_column_values_to_not_be_null("admission_date"))
        
        # 2. Value ranges
        results.append(ge_df.expect_column_values_to_be_between("age", 0, 120))
        results.append(ge_df.expect_column_values_to_be_between("cost", 0, 1000000))
        
        # 3. Categorical values
        results.append(ge_df.expect_column_values_to_be_in_set("gender", ["M", "F", "O"]))
        results.append(ge_df.expect_column_values_to_be_in_set("diagnosis_code", 
            ["I10", "E11.9", "F32.9", "M54.5", "J06.9", "Z00.00"]))
        
        # Analyze results
        success = all(res['success'] for res in results)
        
        if success:
            log.info("Silver layer validation PASSED")
        else:
            failed_cols = [res['expectation_config']['kwargs'].get('column') 
                          for res in results if not res['success']]
            log.warning(f"Silver layer validation FAILED for columns: {failed_cols}")
            
        return success

    def validate_gold_layer(self, df: DataFrame) -> bool:
        """
        Validates Gold layer aggregations.
        """
        log.info("Starting Gold layer validation")
        ge_df = ge.dataset.SparkDFDataset(df)
        
        results = []
        # Patient count should be at least 1
        results.append(ge_df.expect_column_values_to_be_between("patient_count", 1, 10000))
        # Total billing should be positive
        results.append(ge_df.expect_column_values_to_be_between("total_billing", 0, 100000000))
        
        success = all(res['success'] for res in results)
        if success:
            log.info("Gold layer validation PASSED")
        else:
            log.warning("Gold layer validation FAILED")
            
        return success
