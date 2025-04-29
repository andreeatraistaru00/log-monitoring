import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

LOG_FILE = Path("logs.log")
REPORT_FILE = Path("report.log")

WARNING_THRESHOLD = 5
ERROR_THRESHOLD = 10

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("[%(levelname)s] %(message)s")

file_handler = logging.FileHandler(REPORT_FILE, encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def process_logs(file_path: Path) -> None:

  active_jobs: Dict[str, Tuple[datetime, str]] = {}

  with file_path.open(newline="") as fp:
    reader = csv.reader(fp)

    for row in reader:
      time_str, description, status, pid = [x.strip() for x in row]

      try:
        timestamp = datetime.strptime(time_str, "%H:%M:%S")
      except ValueError:
        logger.warning("Invalid timestamp '%s' in row: %r", time_str, row)

      if status == "START":
        active_jobs[pid] = (timestamp, description)

      elif status == "END":

        if pid in active_jobs:
          start_time, desc = active_jobs.pop(pid)
          duration = (timestamp - start_time).total_seconds() / 60
          
          if duration > ERROR_THRESHOLD:
            logger.error("Job '%s' (PID %s) took %.2f minutes", desc, pid, duration)
          elif duration > WARNING_THRESHOLD:
            logger.warning("Job '%s' (PID %s) took %.2f minutes", desc, pid, duration)

        else:
          logger.info("END event with no START for PID %s", pid)
      
      else:
        logger.info("Unknown status for job with PID %s", pid)

 
if __name__ == "__main__":
  """
  Entry point: process log file
  """
  process_logs(LOG_FILE)
  
