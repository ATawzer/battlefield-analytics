# Main scraper client interface. Creates a scraper capable of hitting pages
from bs4 import BeautifulSoup
import os
import time
import logging
l = logging.getLogger('crawler')

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

    def crawl(self, cycles=1, starting_point=None, platform='ps4'):
        """
        Cycles through multiple report and user crawl sessions.
        Sepcifying a user will serve as a jump off point
        """

        if starting_point is not None:
            self.crawl_reports_from_player_report_url(starting_point)

        for i in range(cycles):

            l.info(f"Running cycle {i}/{cycles}")
            self.crawl_empty_reports(platform)
            self.crawl_empty_players()

    def crawl_reports_from_player_report_url(self, url):
        """
        Given a player report url will write all the matches back into the DB.
        """

        user_reports = self.user_reports_scraper.get(url)
        parsed_reports = self.user_reports_parser.parse(BeautifulSoup(user_reports))

        self.bdb.post_many_game_reports(parsed_reports, parsed=False)

    def crawl_empty_players(self):
        """
        Gets players that haven't been scraped in a week
        """

        players = self.bdb.get_unscraped_players(days=7)

        if len(players) > 0:

            l.info(f"Found {len(players)} to scrape.")
            for player in players:

                url = f"https://battlelog.battlefield.com/bf4/soldier/{player['gamertag']}/battlereports/{player['persona_id']}/{player['platform']}/"
                self.crawl_reports_from_player_report_url(url)

                time.sleep(15)
        else:
            l.info("No Empty Players found. Moving on.")


    def crawl_empty_reports(self, platform):
        """
        Crawls over empty game reports in the database.
        """

        # scrape game reports
        reports = self.bdb.get_game_reports(fields=['game_id', 'data_platform'], parsed=False)

        if len(reports)>0:

            l.info(f"Found {len(reports)} to scrape")
            # Scrape, parse and record
            for i, report in enumerate(reports):

                # Scrape
                url = f"https://battlelog.battlefield.com/bf4/battlereport/show/{report['data_platform']}/{report['game_id']}/"
                scraped_report = self.report_scraper.get(url)

                # Parse Report
                try:
                    parsed_report = self.report_parser.parse(BeautifulSoup(scraped_report))
                except:
                    l.debug(f"Failed to Parse {url}")
                    continue

                # Write to DB
                parsed_report['parsed'] = True
                self.bdb.post_game_report(parsed_report, parsed=True)
                self.bdb.post_game_report_players(parsed_report['player_data'], platform=platform)

                # Wait to repeat
                time.sleep(15)
        else:
            l.info("No empty reports, moving on.")