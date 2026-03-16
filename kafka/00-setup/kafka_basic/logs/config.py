import logging
import os
from dotenv import load_dotenv

load_dotenv()
def setup_logging(
    log_file = os.getenv('LOG_FILE'),
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
):
    logging.basicConfig(
        filename=log_file,
        level=level,
        format=log_format,
        encoding='utf-8'
    )