import mlflow
from src.config import settings
from src.utils.logging_config import log

def setup_mlflow():
    """
    Initializes MLflow configuration and experiment.
    """
    try:
        mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
        mlflow.set_experiment(settings.MLFLOW_EXPERIMENT_NAME)
        log.info(f"MLflow initialized: Tracking URI {settings.MLFLOW_TRACKING_URI}, Experiment {settings.MLFLOW_EXPERIMENT_NAME}")
    except Exception as e:
        log.error(f"Failed to initialize MLflow: {str(e)}")

def log_ml_metric(metric_name, value, step=None):
    """
    Logs a metric to the active MLflow run.
    """
    try:
        mlflow.log_metric(metric_name, value, step=step)
    except Exception as e:
        log.error(f"Error logging metric to MLflow: {str(e)}")

def log_ml_param(param_name, value):
    """
    Logs a parameter to the active MLflow run.
    """
    try:
        mlflow.log_param(param_name, value)
    except Exception as e:
        log.error(f"Error logging parameter to MLflow: {str(e)}")
