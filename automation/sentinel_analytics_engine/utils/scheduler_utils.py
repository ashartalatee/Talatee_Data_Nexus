import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

class SchedulerUtils:
    """
    Provides utility functions for pipeline scheduling, execution windows,
    and state tracking for the Talatee Sentinel Engine.
    """
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.client_id = config.get("client_id", "unknown")
        self.state_dir = Path("data/states") / self.client_id
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def should_run(self) -> bool:
        """
        Checks if the current execution time falls within the allowed 
        scheduling window defined in the client config.
        """
        schedule_cfg = self.config.get("scheduling", {})
        if not schedule_cfg:
            self.logger.info("No scheduling constraints found. Proceeding with execution.")
            return True

        allowed_hours = schedule_cfg.get("allowed_hours", range(0, 24))
        current_hour = datetime.now().hour

        if current_hour not in allowed_hours:
            self.logger.warning(f"Execution blocked: Current hour {current_hour} is outside allowed window.")
            return False

        return True

    def get_last_run_timestamp(self) -> Optional[datetime]:
        """
        Retrieves the timestamp of the last successful pipeline completion
        to support incremental data ingestion.
        """
        state_file = self.state_dir / "last_run.txt"
        if not state_file.exists():
            return None
        
        try:
            timestamp_str = state_file.read_text().strip()
            return datetime.fromisoformat(timestamp_str)
        except Exception as e:
            self.logger.error(f"Failed to read last run state: {e}")
            return None

    def update_run_state(self, status: str = "success") -> None:
        """
        Records the completion of a pipeline run to prevent duplicate processing
        and provide audit trails for the agency.
        """
        state_file = self.state_dir / "last_run.txt"
        log_file = self.state_dir / "execution_history.log"
        
        now = datetime.now().isoformat()
        
        try:
            if status == "success":
                state_file.write_text(now)
            
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"{now} | status: {status}\n")
                
            self.logger.info(f"Execution state updated with status: {status}")
        except Exception as e:
            self.logger.error(f"Failed to update execution state: {e}")

    def get_ingestion_window(self) -> Dict[str, datetime]:
        """
        Calculates the start and end timestamps for data fetching based 
        on the last successful run or a default lookback period.
        """
        last_run = self.get_last_run_timestamp()
        default_lookback_days = self.config.get("ingestion", {}).get("default_lookback_days", 7)
        
        end_time = datetime.now()
        start_time = last_run if last_run else (end_time.replace(hour=0, minute=0, second=0, microsecond=0) 
                                               - pd.Timedelta(days=default_lookback_days))
        
        return {
            "start": start_time,
            "end": end_time
        }