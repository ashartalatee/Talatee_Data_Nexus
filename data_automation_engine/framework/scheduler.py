import schedule
import time


class Scheduler:

    def __init__(self, job_function):
        self.job_function = job_function

    def run_daily(self):

        schedule.every().day.at("09:00").do(self.job_function)

        while True:
            schedule.run_pending()
            time.sleep(60)