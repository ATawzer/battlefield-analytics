# Main scraper client interface. Creates a scraper capable of hitting pages
from re import S
from bs4 import BeautifulSoup
import os

# Internal
from .scrapers import *
from .parsers import *
from db.client import BFADB

class BF4GameReportCrawler:
    """
    Orchestrates parsers and getters in succession to scrape
    large swathes of game reports
    """

    def __init__(self, db_client=None, gecko_driver_path=None):

        self.bdb = db_client if db_client is not None else BFADB(from_env=True)

        # Scrapers
        gecko_path = os.get_env('gecko_driver_path') if gecko_driver_path is None else gecko_driver_path
        self.report_scraper = GameReportScraper(gecko_path)
        self.user_reports_scraper = BF4UserReportsScraper(gecko_path)

        # Parser
        self.report_parser = BF4GameReportParser()
        self.user_reports_parser = BF4UserReportsParser()

    def crawl(self, start_url=None):
        """
        Starts at a url or gets reports from the database and 
        loops through users and matches to build out database.
        """

        # scrape game reports
        reports = self.bdb.get_game_reports(parsed=False)

        # Scrape, parse and record
        for i, report in enumerate(reports):

            # Scrape
            url = f"https://battlelog.battlefield.com/bf4/battlereport/show/{report['data_platform']}/{report['game_id']}/"
            scraped_report = self.report_scraper(url)

            # Parse
            parsed_report = self.report_parser(BeautifulSoup(scraped_report))

            # Write to DB


        # Update its record in the database

        # Use report to generate empty player records

        # Write player records

        # Go scrape player reports

        # Parse player reports

        # Add them as blank game reports

        # Repeat
