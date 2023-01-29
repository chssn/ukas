"""
UK AIP Scraper
This contains global imports and global variables
"""

# Python Imports
import os

# 3rd Party Imports
from loguru import logger

# Local Imports

WORK_DIR = os.path.dirname(__file__)
COUNTRY_CODE = "EG"

logger.info("Working directory is {}", WORK_DIR)
