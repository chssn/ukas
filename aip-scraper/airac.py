"""
UK AIP Scraper
"""

# Python Imports
from datetime import date, timedelta
from math import floor

# 3rd Party Imports
from loguru import logger

# Local Imports

BASE_URL = "https://www.aurora.nats.co.uk/htmlAIP/Publications/"
BASE_POST_STRING = "-AIRAC/html/eAIP/"


class Airac:
    '''Class for general functions relating to AIRAC'''

    def __init__(self):
        # First AIRAC date following the last cycle length modification
        start_date = "2019-01-02"
        self.base_date = date.fromisoformat(str(start_date))
        # Length of one AIRAC cycle
        self.cycle_days = 28

    def initialise(self, date_in:str=None) -> int:
        """Calculate the number of AIRAC cycles between any given date and the start date"""

        if date_in is not None:
            # convert the input string to a date
            input_date = date.fromisoformat(str(date_in))
        else:
            input_date = date.today()

        # How many AIRAC cycles have occured since the start date
        diff_cycles = (input_date - self.base_date) / timedelta(days=1)
        # Round that number down to the nearest whole integer
        number_of_cycles = floor(diff_cycles / self.cycle_days)

        return number_of_cycles

    def current_cycle(self) -> date:
        """Return the date of the current AIRAC cycle"""

        number_of_cycles = self.initialise()
        number_of_days = number_of_cycles * self.cycle_days + 1
        current_cycle = self.base_date + timedelta(days=number_of_days)
        logger.info("Current AIRAC Cycle is: {}", current_cycle)

        return current_cycle

    def next_cycle(self) -> date:
        """Return the date of the next AIRAC cycle"""

        number_of_cycles = self.initialise()
        number_of_days = (number_of_cycles + 1) * self.cycle_days + 1

        return self.base_date + timedelta(days=number_of_days)

    def url(self, use_next:bool=False) -> str:
        """Return a generated URL based on the AIRAC cycle start date"""

        if use_next:
            # if the 'use_next' variable is passed, generate a URL for the next AIRAC cycle
            base_date = self.next_cycle()
        else:
            base_date = self.current_cycle()

        formatted_url = BASE_URL + str(base_date) + BASE_POST_STRING
        logger.debug(formatted_url)

        return formatted_url
