# Main scraper client interface. Creates a scraper capable of hitting pages
from bs4 import BeautifulSoup
import os
import time

# Internal
from .scrapers import *
from .parsers import *
from .db import BFADB

class BF4GameReportCrawler:
    """
    Orchestrates parsers and getters in succession to scrape
    large swathes of game reports
    """

    def __init__(self, db_client=None, gecko_driver_path=None):

        self.bdb = db_client if db_client is not None else BFADB(from_env=True)

        # Scrapers
        gecko_path = os.getenv('gecko_driver_path') if gecko_driver_path is None else gecko_driver_path
        self.report_scraper = GameReportScraper(gecko_path)
        self.user_reports_scraper = BF4UserReportsScraper(gecko_path)

        # Parser
        self.report_parser = BF4GameReportParser()
        self.user_reports_parser = BF4UserReportsParser()

    def crawl_reports_from_player_report_url(self, url):
        """
        Given a player report url will write all the matches back into the DB.
        """

        user_reports = self.user_reports_scraper(url)
        parsed_reports = self.user_reports_parser(BeautifulSoup(user_reports))

        self.bdb.post_many_game_reports(parsed_reports, parsed=False)

    def      

    def crawl_empty_reports(self):
        """
        Crawls over empty game reports in the database.
        """

        # scrape game reports
        reports = self.bdb.get_game_reports(fields=['game_id', 'data_platform'], parsed=False)

        # Scrape, parse and record
        for i, report in enumerate(reports):

            # Scrape
            url = f"https://battlelog.battlefield.com/bf4/battlereport/show/{report['data_platform']}/{report['game_id']}/"
            scraped_report = self.report_scraper(url)

            # Parse Report
            parsed_report = self.report_parser(BeautifulSoup(scraped_report))

            # Write to DB
            parsed_report['parsed'] = True
            self.bdb.post_game_report(parsed_report)
            self.bdb.port_game_report_players(parsed_report['players'])

            # Wait to repeat
            time.sleep(3)