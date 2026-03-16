import time


def run_scheduler(job_function, interval_seconds):

    print(f"\nSCHEDULER STARTED (interval: {interval_seconds} seconds)\n")

    while True:

        print("\nRUNNING PIPELINE...\n")

        job_function()

        print(f"\nWaiting {interval_seconds} seconds for next run...\n")

        time.sleep(interval_seconds)