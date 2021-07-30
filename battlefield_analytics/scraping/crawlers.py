# Main scraper client interface. Creates a scraper capable of hitting pages
from bs4 import BeautifulSoup

# Internal
from .scrapers import BAGetter 
from .parsers import BAParser
from db.client import BFADB

class BF4GameReportCrawler:
    """
    Orchestrates parsers and getters in succession to scrape
    large swathes of game reports
    """

    def __init__(self, db_client=None):

        self.bdb = db_client if db_client is not None else BFADB(from_env=True)

        if self.pause < 1:
            raise ValueError("Pause must be greater than one second to be generous to the web server.")

        # init
        self.scraper = BAGetter()
        self.parser = BAParser()

    def 