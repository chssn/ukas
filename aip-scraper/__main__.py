"""
UK AIP Scraper
"""

# Python Imports

# 3rd Party Imports
from loguru import logger

# Local Imports
from . import scraper, verify

@logger.catch
def main() -> None:
    """The main function"""
    web_scrape = scraper.Webscrape()
    web_scrape.run()

    #verify_sector_file = verify.Verify()
    #verify_sector_file.aerodrome_check()

if __name__ == "__main__":
    logger.info("UK AIP Scraper Starting")
    main()
