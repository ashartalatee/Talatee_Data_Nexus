from pathlib import Path
from datetime import datetime

def save_dataset(df, output_dir):

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    output_file = Path(output_dir) / f"clean_dataset_{timestamp}.csv"

    df.to_csv(output_file, index=False)
    
    return output_file