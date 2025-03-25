import schedule
import time
from etl_pipeline import run_etl

def job():
    print("Running ETL Job...")
    run_etl()

schedule.every().day.at("07:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
