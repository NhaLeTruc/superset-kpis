import importlib.util
import os as _os


# Job module files start with digits, making them unimportable via normal import
# syntax. Use importlib.util to load them by file path.
def _load_job_class(filename: str, class_name: str):
    path = _os.path.join(_os.path.dirname(__file__), filename)
    spec = importlib.util.spec_from_file_location(filename[:-3], path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, class_name)


DataProcessingJob = _load_job_class("01_data_processing.py", "DataProcessingJob")
PerformanceMetricsJob = _load_job_class("03_performance_metrics.py", "PerformanceMetricsJob")

__all__ = ["DataProcessingJob", "PerformanceMetricsJob"]
