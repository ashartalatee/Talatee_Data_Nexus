import json
from datetime import datetime
from pathlib import Path


def save_pipeline_report(report_file, report_data):

    report_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    Path(report_file).parent.mkdir(parents=True, exist_ok=True)

    with open(report_file, "w") as f:
        json.dump(report_data, f, indent=4)

    return report_file