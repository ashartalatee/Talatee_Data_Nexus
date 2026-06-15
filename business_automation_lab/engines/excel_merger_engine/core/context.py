import uuid
import time
from typing import Optional

class EngineContext:
    """
    Menyediakan runtime context untuk memastikan pelacakan (telemetry) 
    dan determinisme eksekusi system terjamin dari awal hingga akhir.
    """
    def __init__(self, execution_mode: str = "PRODUCTION"):
        self.run_id: str = str(uuid.uuid4())
        self.start_time: float = time.time()
        self.execution_mode: str = execution_mode
        self.metrics: dict = {
            "files_processed": 0,
            "rows_merged": 0,
            "errors_encountered": 0
        }

    def get_duration(self) -> float:
        return time.time() - self.start_time

    def increment_metric(self, metric_name: str, count: int = 1):
        if metric_name in self.metrics:
            self.metrics[metric_name] += count